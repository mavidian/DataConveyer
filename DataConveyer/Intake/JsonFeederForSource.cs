//JsonFeederForSource.cs
//
// Copyright © 2018-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Intake
{
   internal class JsonFeederForSource : LineFeederForSource
   {
      /// <summary>
      /// Result reported by AdvanceToTargetPath method
      /// </summary>
      private enum AdvanceResult
      {
         Match,
         MatchInNewCluster,
         EOF
      }

      private readonly JsonReader _jsonReader;

      //Settings determined by the ctor:
      private readonly bool _observeClusters;  //true if non-empty ClusterNode setting is present
      private readonly int _clusterLevel;      //to determine if cluster boundary was crossed during call to AdvanceToTargetPath

      private readonly string _adjustedCumulativeTargetPath;  //path to the target nodes (containing records), e.g. Members/Family/Member/
      private readonly Stack<string> _adjustedPathStack;      //cumulative paths of the nodes (arrays/objects) encountered (to be compared against _adjustedCumulativeTargetPath)

      private int _currClstrCnt;  //cluster counter/number (0=undetermined)

      /// <summary>
      /// Create an instance of the record supplier from XML
      /// </summary>
      /// <param name="reader">Underlying text reader.</param>
      /// <param name="sourceNo"></param>
      /// <param name="settings"></param>
      /// <param name="intakeIsAsync"></param>
      internal JsonFeederForSource(TextReader reader, int sourceNo, string settings, bool intakeIsAsync) : base(reader, sourceNo)
      //Note that reader is passed to base class, even though it is not used there (all relevant methods are overridden, so null could've been passed instead).
      //This is because base.Dispose (called by Dispose in this class) disposes the reader (there is no public JsonReader.Dispose method).
      {
         _jsonReader = new JsonTextReader(reader)
         {
            SupportMultipleContent = true
         };

         var settingDict = settings?.SplitPairsToListOfTuples()?.ToDictionary(t => t.Item1, t => t.Item2);

         // Discrete settings to unpack:
         //    CollectionNode      - "xpath" to the collection of clusters/records (may be null/empty, if top level JSON element is array (not object), then CollectionNode needs to be null/absent, as empty would skip the 1st inner match)
         //    ClusterNode         - "xpath" to cluster node within collection node (null/empty means single record clusters)
         //    RecordNode          - "xpath" to record node within cluster node (or collection node if cluster node is empty)
         // "xpath" is a slash separated list of nodes (objects or arrays); it always relative to the outer node
         var collNodePath = settingDict.GetStringSetting("CollectionNode");
         var clstrNodePath = settingDict.GetStringSetting("ClusterNode");
         var recNodePath = settingDict.GetStringSetting("RecordNode");
         _observeClusters = clstrNodePath != null;

         _adjustedPathStack = new Stack<string>();

         //Determine adjusted cummulative target path, i.e. the path to the (object) nodes with the records to be extracted
         //In general, this is concatenation of CollectionNode, ClusterNode and RecordNode (with some nuanced special cases)
         //Empty CollectionNode and/or ClusterNode results in slash at end, null CollectionNode and/or ClusterNode
         //Empty RecordNode does NOT add slash at end except if ClusterNode is not null/empty; RecordNode can only be null if CollectionNode and ClusterNOde are also null (results in multiple root objects).
         //Examples:
         // Members + null + Member        => Members/Member/
         // Root/Members// + null + Member => Root/Members///Member/
         // null + null + empty            => empty
         // empty + null + empty           => /
         // Members + Family + Member      => Members/Family/Member/
         // Members + Family + X/Member    => Members/Family/X/Member/
         // null + empty + Member          => /Member/
         // null + Family + empty          => Family//   (!)
         _adjustedCumulativeTargetPath = collNodePath == null ? string.Empty : collNodePath + "/";
         if (_observeClusters) _adjustedCumulativeTargetPath += clstrNodePath + "/";
         _clusterLevel = _adjustedCumulativeTargetPath.Where(c => c == '/').Count();
         _adjustedCumulativeTargetPath += recNodePath;
         if (recNodePath.Any() || _observeClusters && clstrNodePath.Any()) _adjustedCumulativeTargetPath += "/";

         _currClstrCnt = 0; //will stay at 0 (undetermined) unless ClusterNode defined
      }

      public override Tuple<ExternalLine, int> GetNextLine()
      {
         return SupplyNextXrecord()?.ToTuple(_sourceNo);
      }

      public override async Task<Tuple<ExternalLine, int>> GetNextLineAsync()
      {
         var line = await Task.Run(() => SupplyNextXrecord()); //TODO: Consider SupplyNextXrecordAsync (with AdvanceToLocationAsync, etc.) to make async "fine-grained"
         return line?.ToTuple(_sourceNo);
      }

      public override void Dispose()
      {
         _jsonReader.Close();
         ((IDisposable)_jsonReader).Dispose();
         base.Dispose();
      }


      private Xrecord SupplyNextXrecord()
      {
         var advanceResult = AdvanceToTargetPath();

         if (advanceResult == AdvanceResult.MatchInNewCluster) _currClstrCnt++;

         return advanceResult == AdvanceResult.EOF ? null : GetCurrentXrecord();
      }


      /// <summary>
      /// Consume the remainder of input stream, swallow its contents and the dispose the input
      /// </summary>
      public void ReadToEnd()
      {
         while (_jsonReader.Read()) { }
      }


      /// <summary>
      /// Read _jsonReader until at adjusted cummulative target path location
      /// </summary>
      /// <returns>Match or MatchInNewCluster if succeeded; EOF if location not found and no more data.</returns>
      private AdvanceResult AdvanceToTargetPath()
      {
         var minLevel = _clusterLevel;  //relevant only if _observeClusters

         string lastProp = null;
         while (_jsonReader.Read())
         {
            var tokenType = _jsonReader.TokenType;

            if (tokenType == JsonToken.StartObject || tokenType == JsonToken.StartArray)
            {
               Debug.Assert(_adjustedPathStack.Count != 0 || lastProp == null);  //beginning object/array in JSON is never preceded by name
               _adjustedPathStack.Push(_adjustedPathStack.Count == 0 ? string.Empty : NewCumulativeTargetPath(_adjustedPathStack.Peek(), lastProp, tokenType == JsonToken.StartArray));
            }
            if (tokenType == JsonToken.EndObject || tokenType == JsonToken.EndArray)
            {
               _adjustedPathStack.Pop();
            }

            if (_observeClusters) minLevel = Math.Min(minLevel, _adjustedPathStack.Count == 0 ? 0 : _adjustedPathStack.Peek().Where(c => c == '/').Count());

            if (IsStartElement(tokenType) && _adjustedPathStack.Peek() == _adjustedCumulativeTargetPath)
            {  //start of new record found, just verify if we have crossed cluster boundary in the process
               return _observeClusters && minLevel < _clusterLevel ? AdvanceResult.MatchInNewCluster : AdvanceResult.Match;
            }

            lastProp = tokenType == JsonToken.PropertyName ? (string)_jsonReader.Value : null;
         }

         //end of stream
         return AdvanceResult.EOF;
      }


      /// <summary>
      /// Determine whether a given JSON token is considered a start element
      /// </summary>
      /// <param name="token"></param>
      /// <returns></returns>
      private bool IsStartElement(JsonToken token)
      {
         switch (token)
         {  //note that JsonToken.StartArray is not considered start element(!)
            //TODO: Consider other tokens that may be considered as start elements
            case JsonToken.StartObject:
            case JsonToken.String:
               return true;
            default:
               return false;
         }
      }


      /// <summary>
      /// Determine adjusted cummulative target path based on the previous path and the name of new node
      /// </summary>
      /// <param name="oldPath"></param>
      /// <param name="newNode"></param>
      /// <param name="isArray"></param>
      /// <returns></returns>
      private string NewCumulativeTargetPath(string oldPath, string newNode, bool isArray)
      {
         var retVal = new StringBuilder(oldPath);
         if (isArray)
         {
            retVal.Append(newNode);
            retVal.Append('[');
         }
         else  //isObject (or other non-array?)
         {  //if object nested in array, the trailing part of path will contain 1 (or more) array marks ([)  (also, newNode will be null in this case)
            //replace these array marks ([) by object marks (/), 
            //the idea is to have a uniform "level" concept, so for example an array with a single object can be represented just by the object
            int howMany = 0; //number of array markers at end (to be replaced by object markers)
            while (retVal.Length > 0 && retVal[retVal.Length - 1] == '[') { retVal.Length--; howMany++; Debug.Assert(newNode == null); }
            for (int i = 1; i < howMany; i++) retVal.Append('/'); //one less / than [ as one more will be added below
            retVal.Append(newNode);
            retVal.Append('/');
         }
         return retVal.ToString();
      }


      /// <summary>
      /// Create a record from the nodes inside current node
      /// </summary>
      /// <returns>Record created</returns>
      private Xrecord GetCurrentXrecord()
      {
         //_jsonReader is expected here at the starting element node
         Debug.Assert(_jsonReader.TokenType == JsonToken.StartObject);

         var xrecordItems = new List<Tuple<string, object>>();
         var elemDepth = _jsonReader.Depth;

         //Add record items from inner elements
         AddXrecordItems(xrecordItems, elemDepth, string.Empty);  //this will add items to xrecordItems

         return new Xrecord(xrecordItems, _currClstrCnt);
      }


      /// <summary>
      /// Recursive method to add items to a given record
      /// </summary>
      /// <param name="itemsSoFar">List of record items to add new items to</param>
      /// <param name="elemDepth">Depth (level) of the record element</param>
      /// <param name="keySoFar">Name prefix "accumulated" so far, empty except for nested items, i.e. recursive call where higher level item keys are prefixed with a . separator, e.g. Name.Last</param>
      private void AddXrecordItems(List<Tuple<string, object>> itemsSoFar, int elemDepth, string keySoFar)
      {
         //TODO: Consider making itemsSoFar List<Tuple<string, object>> as JSON contents doesn't have to be string
         Debug.Assert(_jsonReader.TokenType == JsonToken.StartObject);  //each recursive call must be at the record start

         string currentKey = string.Empty;

         while (_jsonReader.Read() && _jsonReader.Depth > elemDepth)
         {
            switch (_jsonReader.TokenType)
            {
               case JsonToken.StartObject: //recursive call to inner level
                  AddXrecordItems(itemsSoFar, elemDepth + 1, currentKey);
                  break;
               case JsonToken.EndObject:
               case JsonToken.StartArray:  //in case of array, dup keys will be added (to be handled by KeyValRecord ctor)
               case JsonToken.EndArray:
                  break;
               case JsonToken.PropertyName:  //accumulate key (dot notation)
                  Debug.Assert(_jsonReader.ValueType == typeof(string));
                  currentKey = string.IsNullOrEmpty(keySoFar) ? (string)_jsonReader.Value : keySoFar + "." + (string)_jsonReader.Value;
                  break;
               default:  //must be a data element, e.g. string
                  itemsSoFar.Add(Tuple.Create(currentKey, _jsonReader.Value));
                  break;
            }
         }
         Debug.Assert(_jsonReader.TokenType == JsonToken.EndObject);
         //here, we should be at the end of object containing items just added
         //If so, remove top path from the path stack (which was pushed in AdvanceToTargetPath)
         //       note that if we are in inner level (recursive call) then nothing was pushed at object start (so nothing to pop here)
         if (_jsonReader.TokenType == JsonToken.EndObject && keySoFar.Length == 0) _adjustedPathStack.Pop();
      }

   }
}
