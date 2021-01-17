﻿//UnboundJsonDispenserForTarget.cs
//
// Copyright © 2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   ///  Writer for unbound JSON to a single target.
   /// </summary>
   internal class UnboundJsonDispenserForTarget : LineDispenserForTarget, ILogAware
   {
      private readonly JsonWriter _jsonWriter;
      private readonly TextWriter _underlyingWriter;  //may be used to enhance "pretty-print"

      // Settings to be determined by the ctor:
      private readonly bool _produceMultipleObjects; // if present, the output contains concatenated JSON objects (object=record) or arrays (array=cluster) - technically not a valid JSON, but commonly used; if absent (default), single array on output.
      private readonly bool _produceClusters; // if present, all objects (records) are enclosed in arrays, where each array represents a cluster; if absent (default), clusters are ignored.
      private readonly bool _skipColumnPresorting; // if present, no pre-sorting of keys (column names) occurs - performance feature, but JSON hierarchy may be off; if absent (default), columns are groupped by segments (in order of appearance.

      //////Discrete settings determined by the ctor (with possible adjustments in InitiateDispensing):
      ////private readonly List<string> _recNodePath;
      ////private List<string> _collNodePath, _clstrNodePath;
      ////private bool _observeClusters;  //true if non-empty ClusterNode setting is present

      ////private int _collNodeCount, _clstrNodeCount, _recNodeCount;  //number of nested nodes constituting CollectionNode, ClusterNode and RecordNode paths respectively

      private int _currClstrNo;  // 1-based last cluster number (0=before 1st cluster)
      private bool _atStart;
      private bool _after1stRecord;

      /// <summary>
      /// Creates an instance of a JSON dispenser for a single target.
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="targetNo">1 based target number.</param>
      /// <param name="settings"></param>
      internal UnboundJsonDispenserForTarget(TextWriter writer, int targetNo, string settings) : base(writer, targetNo)
      //Note that writer is passed to base class, even though it is not used there (all relevant methods are overridden, so null could've been passed instead).
      //This is because base.Dispose (called by Dispose in this class) disposes the writer (it is unclear whether XmlWriter.Dispose disposes underlying writer).
      {

         var settingDict = settings?.SplitPairsToListOfTuples()?.ToDictionary(t => t.Item1, t => t.Item2);

         //Settings:
         //    ProduceMultipleObjects - if present, multiple JSON objects are produced on output (technically not a valid JSON); if absent, a JSON array is produced (each element is an object, i.e. a record)
         //    ProduceClusters        - if present, all objects (records) are enclosed in arrays, where each array represents a cluster; if absent (default), clusters are ignored.
         //    SkipColumnPresorting   - if present, the keys (field names) are processed in order appearing (better perfrormance); if absent, keys are groupped by segments to assure proper JSON hierarchy nesting 
         //    IndentChars            - string to use when indenting, e.g. "\t" or "  "; allows "pretty-print" JSON output; when absent, no indenting takes place. Due to JSON.NET library limitation, the string must consist of identical characters.

         _produceMultipleObjects = settingDict.ContainsKey("ProduceMultipleObjects");
         _produceClusters = settingDict.ContainsKey("ProduceClusters");
         _skipColumnPresorting = settingDict.ContainsKey("SkipColumnPresorting");

         ////// Discrete settings to unpack:
         //////    CollectionNode   - "xpath" defining the collection of clusters/records (may be null/empty, in which case output will contain XML fragment where each root constitutes record or cluster).
         //////    ClusterNode      - "xpath" defining clusters node within collection node (null/empty means record nodes are directly inside collection node).
         //////    RecordNode       - "xpath" defining record node within cluster node (or collection node if cluster node is empty). RecordNode is mandatory, if absent "__record__" is assumed.
         //////    AttributeFields  - a semicolon-separated list of field names (item keys) to be projected into XML as attributes of the record node (and not inner nodes).
         //////    IndentChars      - string to use when indenting, e.g. "\t" or "  "; allows "pretty-print" JSON output; when absent, no indenting takes place. Due to JSON.NET library limitation, the string must consist of identical characters.
         ////// "xpath" is always relative (no need for ./), each of the nodes is separated by /.
         ////_collNodePath = settingDict.GetStringSetting("CollectionNode").ToJsonNodePath();
         ////_clstrNodePath = settingDict.GetStringSetting("ClusterNode").ToJsonNodePath();
         ////_recNodePath = settingDict.GetStringSetting("RecordNode").ToJsonNodePath();
         var indentChars = settingDict.GetStringSetting("IndentChars");
         
         ////_observeClusters = _clstrNodePath?.Any() ?? false;  //same as _clstrNodePath != null && _clstrNodePath.Any()
         ////_collNodeCount = _clstrNodeCount = _recNodeCount = 0;  //will be set in InitiateDispensing

         var jsonWriter = new JsonTextWriter(writer);
      
         if (string.IsNullOrEmpty(indentChars))
         {
            jsonWriter.Formatting = Formatting.None;
         }
         else
         {
            jsonWriter.IndentChar = indentChars[0];
            jsonWriter.Indentation = indentChars.Length;  //note that this will only work as expected if IntentChars setting contains the same char repeated (Newtonsoft.Json limitation)
            jsonWriter.Formatting = Formatting.Indented;
         }
         
         _jsonWriter = jsonWriter;
         _underlyingWriter = writer;

         _currClstrNo = 0;
         _atStart = true;
         _after1stRecord = false;
      }


      /// <summary>
      /// Send a single line (Xrecord) to the target file.
      /// </summary>
      /// <param name="linePlus">Xrecord to send along with target number.</param>
      public override void SendNextLine(Tuple<ExternalLine, int> linePlus)
      {
         Debug.Assert(linePlus != null);  //Note that EOD marks are handled by the owner class, i.e. LineDispenser

         var line = linePlus.Item1;
         Debug.Assert(line.GetType() == typeof(Xrecord));
         Debug.Assert(linePlus.Item2 == TargetNo);  //note that LineDispenserForTarget class could've just be sent ExternalLine (and not the Tuple with TargetNo), but it's kept for consistency/duality with LineFeederForSource

         if (_atStart)  //Very first call
         {
            ////_atStart = false;  //not thread-safe, but single-threaded
            InitiateDispensing(line.ClstrNo);
         }

         ////if (_observeClusters && line.ClstrNo != _currClstrNo)  //new cluster
         ////{  //Close _clstrNodePath and start a new one
         ////   Debug.Assert(_clstrNodePath.Any());
         ////   WriteCloseNodes(_recNodeCount);
         ////   WriteStartNodes(_recNodePath, true);  //no need to update _recNodeCount (it's been aleady set in InitiateDispensign)
         ////   _currClstrNo = line.ClstrNo;
         ////}


         if (_produceClusters && line.ClstrNo != _currClstrNo)
         {
            if (!_atStart) _jsonWriter.WriteEndArray();
            _jsonWriter.WriteStartArray();
            _currClstrNo = line.ClstrNo;
         }

         WriteXrecord(line); //write record contents

         _atStart = false;
      }


      /// <summary>
      /// Asynchronously send a single line (Xrecord) to the target file.
      /// </summary>
      /// <param name="linePlus">Xrecord to send along with target number.</param>
      /// <returns></returns>
      public override async Task SendNextLineAsync(Tuple<ExternalLine, int> linePlus)
      {
         Debug.Assert(linePlus != null);  //Note that EOD marks are handled by the owner class, i.e. LineDispenser

         var line = linePlus.Item1;
         Debug.Assert(line.GetType() == typeof(Xrecord));
         Debug.Assert(linePlus.Item2 == TargetNo);  //note that LineDispenserForTarget class could've just be sent ExternalLine (and not the Tuple with TargetNo), but it's kept for consistency/duality with LineFeederForSource

         if (_atStart)  //Very first call
         {
            _atStart = false;  //not thread-safe, but single-threaded
            await InitiateDispensingAsync(line.ClstrNo);
         }

         //TODO: implement

         ////if (_observeClusters && line.ClstrNo != _currClstrNo)  //new cluster
         ////{  //Close _clstrNodePath and start a new one
         ////   Debug.Assert(_clstrNodePath.Any());
         ////   await WriteCloseNodesAsync(_recNodeCount);
         ////   await WriteStartNodesAsync(_recNodePath, true);  //no need to update _recNodeCount (it's been aleady set in InitiateDispensign)
         ////   _currClstrNo = line.ClstrNo;
         ////}

         await WriteXrecordAsync(line); //write record contents
      }


      /// <summary>
      /// Write closing brackets at end of data.
      /// </summary>
      internal override void ConcludeDispensing()
      {
         //This method is intended to be called by LineDispenser (owner) upon receiving EOD mark. At that point, we are at end of
         // last record - let's close open nodes (we're closing them explicitly even though XmlWriter could do it upon closing).
         if (_atStart) return;  //unlikely, but possible, e.g. 2nd target never directed to
         if (_produceClusters) _jsonWriter.WriteEndArray();
         if (!_produceMultipleObjects) _jsonWriter.WriteEndArray();
         ////       WriteCloseNodes(_collNodeCount + _clstrNodeCount + _recNodeCount);
         _jsonWriter.Flush();
      }


      /// <summary>
      /// Asynchronously write closing brackets at end of data.
      /// </summary>
      /// <returns></returns>
      internal override async Task ConcludeDispensingAsync()
      {
         //This method is intended to be called by LineDispenser (owner) upon receiving EOD mark. At that point, we are at end of
         // last record - let's close open nodes (we're closing them explicitly even though XmlWriter could do it upon closing).

         //TODO: implement

         if (_atStart) return;  //unlikely, but possible, e.g. 2nd target never directed to 
         ////await WriteCloseNodesAsync(_collNodeCount + _clstrNodeCount + _recNodeCount);
         await _jsonWriter.FlushAsync();
      }


      /// <summary>
      /// Dispose underlying JSON writer.
      /// </summary>
      public override void Dispose()
      {
         _jsonWriter.Close();
         ((IDisposable)_jsonWriter).Dispose();
         base.Dispose();
      }


      /// <summary>
      /// Write nodes at the very beginning of the JSON document.
      /// </summary>
      /// <param name="firstClstrNo"></param>
      private void InitiateDispensing(int firstClstrNo)
      {
         if (!_produceMultipleObjects) _jsonWriter.WriteStartArray();


         //////Special case: _recNodePath may be absent/null to indicate each record being its own JSON object without any sort of wrapper
         //////              (akin to XML fragments, technically not a valid JSON)
         //////              If so, then CollectionNode and ClusterNode must also be null.
         ////if (!_recNodePath?.Any() ?? true)
         ////{ //here,  RecordNode is absent; check CollectionNode and ClusterNodes, if any is present then reset it
         ////   if (_collNodePath?.Any() ?? false)
         ////   {  //Collection node is present
         ////      _collNodePath = null;
         ////      this.LogWarning("CollectionNode cannot be present in JSON output settings if RecordNode is absent; CollectionNode is being ignored.");
         ////   }
         ////   if (_observeClusters)
         ////   {  //ClusterNode is present
         ////      _clstrNodePath = null;
         ////      _observeClusters = false;
         ////      this.LogWarning("ClusterNode cannot be present in JSON output settings if RecordNode is absent; ClusterNode is being ignored.");
         ////   }
         ////}

         ////if (_collNodePath?.Any() ?? false)
         ////{  //Collection node is present
         ////   _collNodeCount = WriteStartNodes(_collNodePath, false);
         ////}

         ////if (_observeClusters)
         ////{  //ClusterNode is present
         ////   _clstrNodeCount = WriteStartNodes(_clstrNodePath, true);
         ////   _currClstrNo = firstClstrNo;
         ////}

         ////if (_recNodePath?.Any() ?? false)
         ////{  //RecordNode is present
         ////   _recNodeCount = WriteStartNodes(_recNodePath, true);
         ////}
         ////else Debug.Assert(!_collNodePath?.Any() ?? true && !_observeClusters);
      }


      /// <summary>
      /// Asynchronously write nodes at the very beginning of the JSON document.
      /// </summary>
      /// <param name="firstClstrNo"></param>
      /// <returns></returns>
      private async Task InitiateDispensingAsync(int firstClstrNo)
      {
         //TODO: implement

         //////Special case: _recNodePath may be absent/null to indicate each record being its own JSON object without any sort of wrapper
         //////              (akin to XML fragments, technically not a valid JSON)
         //////              If so, then CollectionNode and ClusterNode must also be null.
         ////if (!_recNodePath?.Any() ?? true)
         ////{ //here,  RecordNode is absent; check CollectionNode and ClusterNodes, if any is present then reset it
         ////   if (_collNodePath?.Any() ?? false)
         ////   {  //Collection node is present
         ////      _collNodePath = null;
         ////      this.LogWarning("CollectionNode cannot be present in JSON output settings if RecordNode is absent; CollectionNode is being ignored.");
         ////   }
         ////   if (_observeClusters)
         ////   {  //ClusterNode is present
         ////      _clstrNodePath = null;
         ////      _observeClusters = false;
         ////      this.LogWarning("ClusterNode cannot be present in JSON output settings if RecordNode is absent; ClusterNode is being ignored.");
         ////   }
         ////}

         ////if (_collNodePath?.Any() ?? false)
         ////{  //Collection node is present
         ////   _collNodeCount = await WriteStartNodesAsync(_collNodePath, false);
         ////}

         ////if (_observeClusters)
         ////{  //ClusterNode is present
         ////   _clstrNodeCount = await WriteStartNodesAsync(_clstrNodePath, true);
         ////   _currClstrNo = firstClstrNo;
         ////}

         ////if (_recNodePath?.Any() ?? false)
         ////{  //RecordNode is present
         ////   _recNodeCount = await WriteStartNodesAsync(_recNodePath, true);
         ////}
         ////else Debug.Assert(!_collNodePath?.Any() ?? true && !_observeClusters);
      }


      /////// <summary>
      /////// Write a series of start object nodes.
      /////// </summary>
      /////// <param name="nodesToWrite">Nodes constituting collection, cluster or record.</param>
      /////// <param name="arrayAtEnd">true to write array start after the nodes (cluster and record), false otherwise (collection).</param>
      /////// <returns>Number of nodes written.</returns>
      ////private int WriteStartNodes(IEnumerable<string> nodesToWrite, bool arrayAtEnd)
      ////{
      ////   Debug.Assert(nodesToWrite.Any());

      ////   bool nonEmptyNodeExists = false;  //to verify if writing array at end should be skipped

      ////   int retVal = 0;

      ////   foreach (var node in nodesToWrite)
      ////   {
      ////      if (node.Length > 0)
      ////      {
      ////         _jsonWriter.WriteStartObject();
      ////         _jsonWriter.WritePropertyName(node);
      ////         nonEmptyNodeExists = true;
      ////      }
      ////      else //empty node name
      ////      {  // instead of an object with an empty name, write an array
      ////         _jsonWriter.WriteStartArray();
      ////      }
      ////      retVal++;
      ////   }

      ////   // Generally, arrayAtEnd should be true when starting cluster and record, but false for collection.
      ////   // However, if all nodes to write are empty, then don't add array at end.
      ////   // This is because "one empty node serves as no node" - note that null ClusterNode and RecordNode
      ////   // cannot serve as "no node" as it has special meaning ("don't observe clusters" and "each record own JSON" respectively)

      ////   if (arrayAtEnd && nonEmptyNodeExists) { _jsonWriter.WriteStartArray(); retVal++;  }

      ////   return retVal;
      ////}


      /////// <summary>
      /////// Asynchronously write a series of start object nodes.
      /////// </summary>
      /////// <param name="nodesToWrite">Nodes constituting collection, cluster or record.</param>
      /////// <param name="arrayAtEnd">true to write array start after the nodes (cluster and record), false otherwise (collection).</param>
      /////// <returns>Task with a number of start nodes written.</returns>
      ////private async Task<int> WriteStartNodesAsync(IEnumerable<string> nodesToWrite, bool arrayAtEnd)
      ////{
      ////   Debug.Assert(nodesToWrite.Any());

      ////   bool nonEmptyNodeExists = false;  //to verify if writing array at end should be skipped

      ////   int retVal = 0;

      ////   foreach (var node in nodesToWrite)
      ////   {
      ////      if (node.Length > 0)
      ////      {
      ////         await _jsonWriter.WriteStartObjectAsync();
      ////         await _jsonWriter.WritePropertyNameAsync(node);
      ////         nonEmptyNodeExists = true;
      ////      }
      ////      else //empty node name
      ////      {  // instead of an object with an empty name, write an array
      ////         await _jsonWriter.WriteStartArrayAsync();
      ////      }
      ////      retVal++;
      ////   }

      ////   // Generally, arrayAtEnd should be true when starting cluster and record, but false for collection.
      ////   // However, if all nodes to write are empty, then don't add array at end.
      ////   // This is because "one empty node serves as no node" - note that null ClusterNode and RecordNode
      ////   // cannot serve as "no node" as it has special meaning ("don't observe clusters" and "each record own JSON" respectively)

      ////   if (arrayAtEnd && nonEmptyNodeExists) { await _jsonWriter.WriteStartArrayAsync(); retVal++; }

      ////   return retVal;
      ////}


      /////// <summary>
      /////// Write a series of ending brackets.
      /////// </summary>
      /////// <param name="howMany">Number of consecutive nodes (levels, i.e. arrays or objects) to close.</param>
      ////private void WriteCloseNodes(int howMany)
      ////{
      ////   while (howMany-- > 0) _jsonWriter.WriteEnd();
      ////}


      /////// <summary>
      /////// Asynchronously write a series of ending brackets.
      /////// </summary>
      /////// <param name="howMany">Number of consecutive nodes (levels, i.e. arrays or objects) to close.</param>
      /////// <returns></returns>
      ////private async Task WriteCloseNodesAsync(int howMany)
      ////{
      ////   while (howMany-- > 0) await _jsonWriter.WriteEndAsync();
      ////}


      /// <summary>
      /// Send items of the current record to JSON output.
      /// </summary>
      /// <param name="line"></param>
      private void WriteXrecord(ExternalLine line)
      {
         Debug.Assert(line.GetType() == typeof(Xrecord));

         if (_produceMultipleObjects && _jsonWriter.Formatting == Formatting.Indented && _after1stRecord)
         {  //we're starting a new JSON document here (technically not a valid JSON)
            //add an extra new line to help in "pretty printing"
            _underlyingWriter.WriteLine();
         }
         _jsonWriter.WriteStartObject();

         WriteItems(_skipColumnPresorting ? line.Items : PresortItems(line.Items));

         ////foreach (var item in line.Items)
         ////{
         ////   _jsonWriter.WritePropertyName(item.Item1); //key
         ////   _jsonWriter.WriteValue(item.Item2);  //TODO: Consider strongly typed values (when Item2 becomes of object type, not string)
         ////}
         _jsonWriter.WriteEndObject();
         _after1stRecord = true;
      }


      /// <summary>
      /// Send a sequence of key-value pairs as a JSON hierarchy to a text writer.
      /// </summary>
      /// <param name="items">Key-value pairs where Key reflects path to JSON element using special convention (undersore-delimited hierarchy of element names or array names with indeces). Represents a single record.</param>
      public void WriteItems(IEnumerable<Tuple<string,object>> items)
      {  // Item: Item1 = Key, Item2 = Value
    ///     bool? isArray = null;
         var prevKey = new Stack<LorC>();
         prevKey.Push(new LorC("dummy")); //will get removed
         foreach (var item in items)
         {
            ////if (isArray == null)
            ////{
            ////   isArray = item.Key.StartsWith("[");
            ////   if ((bool)isArray) _jsonWriter.WriteStartArray(); else _jsonWriter.WriteStartObject();
            ////}
            var segments = SplitColumnName(item.Item1);
            var unchangedSegmentsCount = segments.Zip(prevKey.Reverse(), (f, s) => Tuple.Create(f,s)).TakeWhile(t => t.Item1.Equals(t.Item2)).Count();
            while (prevKey.Count > unchangedSegmentsCount + 1)
            {
               var segment = prevKey.Pop();
               if (segment.IsLabel) _jsonWriter.WriteEndObject(); else _jsonWriter.WriteEndArray();
            }
            prevKey.Pop();

            bool first = true;
            foreach (var segment in segments.Skip(unchangedSegmentsCount))
            {
               if (segment.IsLabel)
               {
                  if (!first) _jsonWriter.WriteStartObject();
                  _jsonWriter.WritePropertyName(segment.Label);
               }
               else //IsCounter
               {
                  if (!first) _jsonWriter.WriteStartArray();
               }
               first = false;
               prevKey.Push(segment);
            }
            _jsonWriter.WriteValue(item.Item2);
         }
         ////if (isArray != null)  // null if empty items
         ////{
         ////   if ((bool)isArray) _jsonWriter.WriteEndArray();
         ////   else _jsonWriter.WriteEndObject();
         ////}
      }


      /// <summary>
      /// Sort data in a way to facilitate creation of nested JSON hierarchy.
      /// All top key segments are groupped together (in order of first appearance), then 2nd key segments are groupped together, etc.
      /// </summary>
      /// <param name="items">A sequence of key value pairs where key is a compound column name.</param>
      /// <returns>A sequence of sorted key value pairs.</returns>
      public IEnumerable<Tuple<string,object>> PresortItems(IEnumerable<Tuple<string,object>> items)
      {  // Item: Item1 = Key, Item2 = Value
         var wrappedItems = items.Select(i => Tuple.Create(i.Item1.Split('.', '['), i));  // Item1 is a "wrapper", i.e. an array of segments of the compound Key (note that array indices will contain closing brackets, but it doesn't matter)
         return SortWrappedData(wrappedItems).Select(wi => wi.Item2);
      }


      /// <summary>
      /// Recursive method to sort wrapped items by the key segments.
      /// Wrapped item is a tuple, where Item1 is a wrapper (array of compound key segments) and Item2 is the item itself.
      /// Each iteration groups items by the next-level key segments and outputs those items that have no key segments left.
      /// </summary>
      /// <param name="wrappedItems"></param>
      /// <returns></returns>
      private IEnumerable<Tuple<string[], Tuple<string, object>>> SortWrappedData(IEnumerable<Tuple<string[], Tuple<string,object>>> wrappedItems)
      {  // WrappedItem: Item1 = Wrapper, Item2 = Item
         var grouppedItems = wrappedItems.GroupBy(wi => wi.Item1[0]);  //wrappedItems: Item1 = Wrapper, Item2 = Item
         // Notice that segments are groupped in order of appearance. So, array elements are not sorted by index.
         foreach (var group in grouppedItems)
         {
            if (group.Count() == 1) yield return group.First();  // end of recursion (note that compound keys are assumed to be unique)
            else
            {  // remove top (groupped by) segment from the Wrapper when recursing
               var innerGroup = group.Select(wi => Tuple.Create(wi.Item1.Skip(1).ToArray(), wi.Item2));
               foreach (var wi in SortWrappedData(innerGroup)) yield return wi;
            }
         }
      }


      /// <summary>
      /// Obtain a hierarchy of JSON elements from a compound column name (key).
      /// </summary>
      /// <param name="key">Compound column name.</param>
      /// <returns>A list of elements that the column name represents.</returns>
      private IEnumerable<LorC> SplitColumnName(string key)
      {
         var items = key.TrimStart('[').Split('.', '[');  // array indices will contain closing brackets

         foreach (var item in items)
         {
            yield return char.IsDigit(item[0])
                        ? new LorC(int.Parse(item.TrimEnd(']')))  // counter
                        : new LorC(item);  // label
         }
      }


      /// <summary>
      /// Asynchronously end items of the current record to JSON output.
      /// </summary>
      /// <param name="line"></param>
      /// <returns></returns>
      private async Task WriteXrecordAsync(ExternalLine line)
      {
         Debug.Assert(line.GetType() == typeof(Xrecord));

         if (_jsonWriter.Formatting == Formatting.Indented && _after1stRecord)
         {  //we're starting a new JSON document here (technically not a valid JSON)
            //add an extra new line to help in "pretty printing"
            await _underlyingWriter.WriteLineAsync();
         }
         await _jsonWriter.WriteStartObjectAsync();
         foreach (var item in line.Items)
         {
            //note that unlike XML, JSON does not support attributes, so we know that every item has to be written as inner node
            await _jsonWriter.WritePropertyNameAsync(item.Item1); //key
            await _jsonWriter.WriteValueAsync(item.Item2);  //TODO: Consider strongly typed values (when Item2 becomes of object type, not string)
         }
         await _jsonWriter.WriteEndObjectAsync();
         _after1stRecord = true;
      }

   }
}