//XmlFeederForSource.cs
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
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Xml;

namespace Mavidian.DataConveyer.Intake
{
   internal class XmlFeederForSource : LineFeederForSource
   {
      /// <summary>
      /// State of the reader relative to the collection of records to extract.
      /// Only a single collection (first encountered) is considered.
      /// </summary>
      private enum ReaderState
      {
         BeforeCollection,
         AtCollectionOutOfCluster,  //only if _observeClusters, i.e. ClusterNode is specified
         AtCollectionInCluster,     //if !_observeClusters, it simply means "inside collection", i.e. ready for the record
         AfterCollection
      }
      //TODO: consider refactoring ReaderState.AtCollectionOutOfCluster, which (as explained in comments) below may also mean AfterCollection.

      private readonly XmlReader _xmlReader;

      //Discrete settings determined by the ctor:
      private readonly XmlNodePath _collNodePath, _clstrNodePath, _xrecNodePath;
      private readonly bool _includeExplicitText;
      private readonly bool _includeAttributes;
      private readonly bool _addPrefixToAttrKeys;  // only relevant when _includeAttributes is true
      private readonly bool _observeClusters;  //true if non-empty ClusterNode setting is present

      private readonly bool _addClusterDataToTraceBin;
      private IDictionary<string, object> _traceBin;
      private string _traceBinKeyPrefix;

      private int _currClstrCnt;  //cluster counter/number (0=undetermined); always 0 unless _observeClusters
      private ReaderState _readerState;

      private int _clstrBaseDepth, _recBaseDepth; //level limit when advancing to cluster/record node (i.e. level below collection/cluster node respectively)

      /// <summary>
      /// Create an instance of the record supplier from XML
      /// </summary>
      /// <param name="reader">Underlying text reader.</param>
      /// <param name="sourceNo"></param>
      /// <param name="settings"></param>
      /// <param name="intakeIsAsync"></param>
      internal XmlFeederForSource(TextReader reader, int sourceNo, string settings, bool intakeIsAsync) : base(reader, sourceNo)
      //Note that reader is passed to base class, even though it is not used there (all relevant methods are overridden, so null could've been passed instead).
      //This is because base.Dispose (called by Dispose in this class) disposes the reader (it is unclear whether XmlReader.Dispose disposes underlying reader).
      {
         _xmlReader = XmlReader.Create(reader, new XmlReaderSettings()
         {
            ConformanceLevel = ConformanceLevel.Fragment,
            IgnoreWhitespace = true,
            IgnoreComments = true,
            IgnoreProcessingInstructions = true,
            Async = intakeIsAsync
         });

         var settingDict = settings?.SplitPairsToListOfTuples()?.ToDictionary(t => t.Item1, t => t.Item2);

         // Discrete settings to unpack:
         //    CollectionNode      - "xpath" to the collection of clusters/records (may be null/empty, in which case input stream contains XML fragment where each root constitutes record or cluster)
         //    ClusterNode         - "xpath" to cluster node within collection node (null/empty means single record clusters)
         //    RecordNode          - "xpath" to record node within cluster node (or collection node if cluster node is empty)
         //    IncludeExplicitText - true to include explicit text in RecordNode; false (default) to ignore it
         //    IncludeAttributes   - true to include attributes (prefixed by @); truePlain to include attributes w/o prefix; false (default) to ignore them
         // "xpath" is always relative (no need for ./), each of the nodes (separated by /) can contain attribute
         _collNodePath = new XmlNodePath(settingDict.GetStringSetting("CollectionNode"));
         _clstrNodePath = new XmlNodePath(settingDict.GetStringSetting("ClusterNode"));
         _xrecNodePath = new XmlNodePath(settingDict.GetStringSetting("RecordNode"));
         _includeExplicitText = settingDict.GetStringSetting("IncludeExplicitText")?.ToLower() == "true";
         var inclAttrsSetting = settingDict.GetStringSetting("IncludeAttributes")?.ToLower();
         _includeAttributes = inclAttrsSetting.SafeSubstring(0, 4) == "true";
         _addPrefixToAttrKeys = inclAttrsSetting != "trueplain";
         _observeClusters = !_clstrNodePath.IsEmpty;
         _addClusterDataToTraceBin = settingDict.GetStringSetting("AddClusterDataToTraceBin")?.ToLower() == "true";

         _currClstrCnt = 0; //will stay at 0 (undetermined) unless ClusterNode defined
         _readerState = ReaderState.BeforeCollection;

         _clstrBaseDepth = _recBaseDepth = -1;
      }


      public override Tuple<ExternalLine, int> GetNextLine()
      {
         return SupplyNextXrecord()?.ToTuple(_sourceNo);
      }

      public override async Task<Tuple<ExternalLine, int>> GetNextLineAsync()
      {
         var line = await SupplyNextXrecordAsync();
         return line?.ToTuple(_sourceNo);
      }

      public override void Dispose()
      {
         _xmlReader.Dispose();  //it also does Close
         base.Dispose();
      }


      /// <summary>
      /// Read enough _xmlReader to determine subsequent record.
      /// </summary>
      /// <returns>The next record read.</returns>
      private Xrecord SupplyNextXrecord()
      {
         if (_readerState == ReaderState.BeforeCollection)
         { //at first, reader needs to be placed inside the collection of records to be extracted (note that only a single collection is considered)
            var atCollection = true;
            if (!_collNodePath.IsEmpty) atCollection = AdvanceToLocation(_collNodePath, -1);  // base level is one level above root, i.e. -1
            if (atCollection)
            {
               if (_observeClusters)
               {
                  _readerState = ReaderState.AtCollectionOutOfCluster;
                  _clstrBaseDepth = _collNodePath.IsEmpty ? -1 : CurrentElementDepth();  //if no collection node, then one level above root, i.e. -1
               }
               else
               {
                  _readerState = ReaderState.AtCollectionInCluster;
                  _recBaseDepth = _collNodePath.IsEmpty ? -1 : CurrentElementDepth();  //if no collection node, then one level above root, i.e. -1
               }
            }
            else _readerState = ReaderState.AfterCollection;
         }
         if (_readerState == ReaderState.AfterCollection) return null;  // collection not found

         if (_readerState == ReaderState.AtCollectionInCluster)
         {
            if (!AdvanceToLocation(_xrecNodePath, _recBaseDepth)) _readerState = !_observeClusters || _xmlReader.EOF ? ReaderState.AfterCollection : ReaderState.AtCollectionOutOfCluster;
            // Note that in case of memory stream (underneath _xmlReader), EOF may still be false even after Read() inside AdvanceToLocation returned false (which means no more data);
            // If this happens (and _observeClusters is true), _readerState shows AtCollectionOutOfCluster, even though it should be AfterCollection; this condition is detected inside the call to GetCurrentXrecord below.
         }

         if (_readerState == ReaderState.AtCollectionOutOfCluster)
         { // Advance to the next cluster
           //here, we must be in _observeClusters mode and either at start or after completing a cluster
            Debug.Assert(_observeClusters);
            _currClstrCnt++;
            if (_addClusterDataToTraceBin)
            {
               _traceBin = new Dictionary<string, object>();
               _traceBinKeyPrefix = string.Empty;
            }
            _readerState = AdvanceToLocation(_clstrNodePath, _clstrBaseDepth, -9, null, false, _addClusterDataToTraceBin) ? ReaderState.AtCollectionInCluster : ReaderState.AfterCollection;
            _recBaseDepth = CurrentElementDepth();
            _xmlReader.MoveToContent();
            Debug.Assert(_readerState == ReaderState.AtCollectionInCluster && _xmlReader.IsStartElement() && _xmlReader.Name == _clstrNodePath.NodeDefs.Last().Name
                        || _readerState == ReaderState.AfterCollection);
            if (_readerState == ReaderState.AtCollectionInCluster)
            {
               //We're marking here the first call to AdvaneToLocation for a cluster (last parm firsInCluster=true) - this is complicated/unreliable and may need to be refactored
               if (!AdvanceToLocation(_xrecNodePath, _recBaseDepth, -9, null, true)) _readerState = _xmlReader.EOF ? ReaderState.AfterCollection : ReaderState.AtCollectionOutOfCluster;
               // Note that in case of memory stream (underneath _xmlReader), EOF may still be false even after Read() inside AdvanceToLocation returned false (which means no more data);
               // If this happens, _readerState shows AtCollectionOutOfCluster, even though it should be AfterCollection; this condition is detected inside the call to GetCurrentXrecord below.
            }
         }

         _xmlReader.MoveToContent();  //needed to assure we're not left at attribute in case last AdvanceToLocation involved attribute match
                                      //as an aside, note that Debug.Assert below may call IsStartElement (which in turn calls MoveToContent), but this will not happen in Release

         //here, we must be either on the element start node or after collection
         Debug.Assert(_readerState == ReaderState.AtCollectionInCluster && _xmlReader.IsStartElement() && _xmlReader.Name == _xrecNodePath.NodeDefs.Last().Name
                   || _readerState == ReaderState.AfterCollection
                   || _readerState == ReaderState.AtCollectionOutOfCluster && _xmlReader.Depth == 0); //this last option is really AfterCollection, where Read() returned false, but EOF is not (yet) true (can happen in case of underlying memory stream)

         return _readerState == ReaderState.AfterCollection ? null : GetCurrentXrecord();
      }


      /// <summary>
      /// Asynchronously read enough _xmlReader to determine subsequent record.
      /// </summary>
      /// <returns>Task with the next record read.</returns>
      private async Task<Xrecord> SupplyNextXrecordAsync()
      {
         if (_readerState == ReaderState.BeforeCollection)
         { //at first, reader needs to be placed inside the collection of records to be extracted (note that only a single collection is considered)
            var atCollection = true;
            if (!_collNodePath.IsEmpty) atCollection = await AdvanceToLocationAsync(_collNodePath, -1);  // base level is one level above root, i.e. -1
            if (atCollection)
            {
               if (_observeClusters)
               {
                  _readerState = ReaderState.AtCollectionOutOfCluster;
                  _clstrBaseDepth = _collNodePath.IsEmpty ? -1 : CurrentElementDepth();  //if no collection node, then one level above root, i.e. -1
               }
               else
               {
                  _readerState = ReaderState.AtCollectionInCluster;
                  _recBaseDepth = _collNodePath.IsEmpty ? -1 : CurrentElementDepth();  //if no collection node, then one level above root, i.e. -1
               }
            }
            else _readerState = ReaderState.AfterCollection;
         }
         if (_readerState == ReaderState.AfterCollection) return null;  // collection not found

         if (_readerState == ReaderState.AtCollectionInCluster)
         {
            if (!await AdvanceToLocationAsync(_xrecNodePath, _recBaseDepth)) _readerState = !_observeClusters || _xmlReader.EOF ? ReaderState.AfterCollection : ReaderState.AtCollectionOutOfCluster;
            // Note that in case of memory stream (underneath _xmlReader), EOF may still be false even after ReadAsync() inside AdvanceToLocationAsync returned false (which means no more data);
            // If this happens (and _observeClusters is true), _readerState shows AtCollectionOutOfCluster, even though it should be AfterCollection; this condition is detected inside the call to GetCurrentXrecordAsync below.
         }

         if (_readerState == ReaderState.AtCollectionOutOfCluster)
         { // Advance to the next cluster
           //here, we must be in _observeClusters mode and either at start or after completing a cluster
            Debug.Assert(_observeClusters);
            _currClstrCnt++;
            if (_addClusterDataToTraceBin)
            {
               _traceBin = new Dictionary<string, object>();
               _traceBinKeyPrefix = string.Empty;
            }
            _readerState = await AdvanceToLocationAsync(_clstrNodePath, _clstrBaseDepth, -9, null, false, _addClusterDataToTraceBin) ? ReaderState.AtCollectionInCluster : ReaderState.AfterCollection;
            _recBaseDepth = CurrentElementDepth();
            await _xmlReader.MoveToContentAsync();
            Debug.Assert(_readerState == ReaderState.AtCollectionInCluster && _xmlReader.IsStartElement() && _xmlReader.Name == _clstrNodePath.NodeDefs.Last().Name
                        || _readerState == ReaderState.AfterCollection);
            if (_readerState == ReaderState.AtCollectionInCluster)
            {
               //We're marking here the first call to AdvaneToLocation for a cluster (last parm firstInCluster=true) - this is complicated/unreliable and may need to be refactored
               if (!await AdvanceToLocationAsync(_xrecNodePath, _recBaseDepth, -9, null, true)) _readerState = _xmlReader.EOF ? ReaderState.AfterCollection : ReaderState.AtCollectionOutOfCluster;
               // Note that in case of memory stream (underneath _xmlReader), EOF may still be false even after ReadAsync() inside AdvanceToLocationAsync returned false (which means no more data);
               // If this happens, _readerState shows AtCollectionOutOfCluster, even though it should be AfterCollection; this condition is detected inside the call to GetCurrentXrecord below.
            }
         }

         await _xmlReader.MoveToContentAsync();  //needed to assure we're not left at attribute in case last AdvanceToLocation involved attribute match
                                                 //as an aside, note that Debug.Assert below may call IsStartElement (which in turn calls MoveToContent), but this will not happen in Release

         //here, we must be either on the element start node or after collection
         Debug.Assert(_readerState == ReaderState.AtCollectionInCluster && _xmlReader.IsStartElement() && _xmlReader.Name == _xrecNodePath.NodeDefs.Last().Name
                   || _readerState == ReaderState.AfterCollection
                   || _readerState == ReaderState.AtCollectionOutOfCluster && _xmlReader.Depth == 0); //this last option is really AfterCollection, where Read() returned false, but EOF is not (yet) true (can happen in case of underlying memory stream)

         return _readerState == ReaderState.AfterCollection ? null : await GetCurrentXrecordAsync();
      }


      /// <summary>
      /// Consume the remainder of input stream, i.e. "swallow" its entire contents)
      /// </summary>
      public void ReadToEnd()
      {
         while (_xmlReader.Read()) { }
      }


      /// <summary>
      /// Asynchronously consume the remainder of input stream, i.e. "swallow" its entire contents)
      /// <returns></returns>
      /// </summary>
      public async Task ReadToEndAsync()
      {
         while (await _xmlReader.ReadAsync()) { }
      }


      /// <summary>
      /// Return the level (depth) of the element the reader is currently at.
      /// </summary>
      /// <returns></returns>
      private int CurrentElementDepth()
      {
         //The function is intended to be called after a search for an element node; its result is 
         // meaningful after a successful searach, i.e. the call to AdvanceToLocation that returned true.
         var retVal = _xmlReader.Depth;
         //In case the pattern searched contained attributes, the successful match will be at attribute
         //, which is 1 level deeper than the matched element. Hence, if so, 1 needs to be subtracted.
         if (_xmlReader.NodeType == XmlNodeType.Attribute) retVal--;
         return retVal;
      }


      /// <summary>
      /// Read _xmlReader until at given location.
      /// </summary>
      /// <param name="nodePath">Location path relative to current position.</param>
      /// <param name="baseDepth">Level (Depth) passed at the initial call to limit the scope of search for inner elements.</param>
      /// <param name="depth">Level (Depth) of the initial call this method (intended to only be passed during recursive calls).</param>
      /// <param name="initPath">Location path from the initial call this method (intended to only be passed during recursive calls - needed in case of reset).</param>
      /// <param name="firstInCluster">Indicator that the record is first in cluster (complicated/unreliable - to be refactored).</param>
      /// <param name="addAttrsToTraceBin">Indicator to add collected attribute data to trace bin; applicable only if AddClusterDataToTraceBin (which in turn is only applicable if _observeClusters).</param>
      /// <returns>True if succeeded; false if location not found.</returns>
      private bool AdvanceToLocation(XmlNodePath nodePath, int baseDepth, int depth = -9, XmlNodePath initPath = null, bool firstInCluster = false, bool addAttrsToTraceBin = false)
      {
         Debug.Assert(!nodePath.IsEmpty);

         if (initPath == null) initPath = nodePath;

         var head = nodePath.NodeDefs[0];      //XmlNodeDef representing the first xpath fragment (before the first /), i.e. current node pattern
         var tail = nodePath.NodeDefs.Skip(1).Any() ? new XmlNodePath(nodePath.NodeDefs.Skip(1))  //XmlNodePath representing the xpath fragments after the first /, i.e. the remaining node patterns
                                                    : null;  //end of recursion

         do { } //advance till next element (ignore e.g. embedded text)
         while (_xmlReader.Read() && !_xmlReader.IsStartElement() && _xmlReader.Depth > baseDepth);
         if (_xmlReader.EOF) return false;

         //Distinction between initial level and base level :
         // initDepth - level at the beginning of initial (external) call (then passed with each recursive iteration)
         // baseDepth - level limit when advancing to next element (e.g. if advancing to cluster, it is the collection level) 
         //Always: initDepth >= baseDepth (i.e. each call is made within level limit)

         int initDepth = depth == -9 ? _xmlReader.Depth : depth;  //-9 means initial call from outside (as opposed to recursive call)

         if (MatchFound(head))
         {
            if (addAttrsToTraceBin)
            {
               _traceBinKeyPrefix = CreateKey(_traceBinKeyPrefix);
               GetAttributes(_traceBinKeyPrefix, false).ToList().ForEach(t => _traceBin.Add(t.Item1, t.Item2));
            }
            initDepth++;
            //are we there yet?
            if (tail == null || AdvanceToLocation(tail, baseDepth, initDepth, tail, false, addAttrsToTraceBin)) return true; //success!
         }

         //no match here, attempt to go back to the initial level and start over
         do
         {
            if (_xmlReader.Depth < initDepth) return false;  //beyond the initial level

            if (_xmlReader.Depth == initDepth)
            { //at initial level, try again (or fail search if first in cluster)
               if (_xmlReader.NodeType == XmlNodeType.EndElement && !firstInCluster) return false;
               //TODO: refactor (remove reliance on firstInCluster) - too complicated.
               return AdvanceToLocation(initPath, baseDepth);
            }
         } while (_xmlReader.Read());

         //end of stream, no xpath found
         return false;
      }


      /// <summary>
      /// Asynchronously read _xmlReader until at given location.
      /// </summary>
      /// <param name="nodePath">Location path relative to current position.</param>
      /// <param name="baseDepth">Level (Depth) passed at the initial call to limit the scope of search for inner elements.</param>
      /// <param name="depth">Level (Depth) of the initial call this method (intended to only be passed during recursive calls).</param>
      /// <param name="initPath">Location path from the initial call this method (intended to only be passed during recursive calls - needed in case of reset).</param>
      /// <param name="firstInCluster">Indicator that the record is first in cluster (complicated/unreliable - to be refactored).</param>
      /// <param name="addAttrsToTraceBin">Indicator to add collected attribute data to trace bin; applicable only if AddClusterDataToTraceBin (which in turn is only applicable if _observeClusters).</param>
      /// <returns>Task with true if succeeded or false if location not found.</returns>
      private async Task<bool> AdvanceToLocationAsync(XmlNodePath nodePath, int baseDepth, int depth = -9, XmlNodePath initPath = null, bool firstInCluster = false, bool addAttrsToTraceBin = false)
      {
         Debug.Assert(!nodePath.IsEmpty);

         if (initPath == null) initPath = nodePath;

         var head = nodePath.NodeDefs[0];      //XmlNodeDef representing the first xpath fragment (before the first /), i.e. current node pattern
         var tail = nodePath.NodeDefs.Skip(1).Any() ? new XmlNodePath(nodePath.NodeDefs.Skip(1))  //XmlNodePath representing the xpath fragments after the first /, i.e. the remaining node patterns
                                                    : null;  //end of recursion

         do { } //advance till next element (ignore e.g. embedded text)
         while (await _xmlReader.ReadAsync() && !_xmlReader.IsStartElement() && _xmlReader.Depth > baseDepth);
         if (_xmlReader.EOF) return false;

         //Distinction between initial level and base level :
         // initDepth - level at the beginning of initial (external) call (then passed with each recursive iteration)
         // baseDepth - level limit when advancing to next element (e.g. if advancing to cluster, it is the collection level) 
         //Always: initDepth >= baseDepth (i.e. each call is made within level limit)

         int initDepth = depth == -9 ? _xmlReader.Depth : depth;  //-9 means initial call from outside (as opposed to recursive call)

         if (MatchFound(head))
         {
            if (addAttrsToTraceBin)
            {
               _traceBinKeyPrefix = CreateKey(_traceBinKeyPrefix);
               GetAttributes(_traceBinKeyPrefix, false).ToList().ForEach(t => _traceBin.Add(t.Item1, t.Item2));
            }
            initDepth++;
            //are we there yet?
            if (tail == null || await AdvanceToLocationAsync(tail, baseDepth, initDepth, tail, false, addAttrsToTraceBin)) return true; //success!
         }

         //no match here, attempt to go back to the initial level and start over
         do
         {
            if (_xmlReader.Depth < initDepth) return false;  //beyond the initial level

            if (_xmlReader.Depth == initDepth)
            { //at initial level, try again (or fail search if first in cluster)
               if (_xmlReader.NodeType == XmlNodeType.EndElement && !firstInCluster) return false;
               //TODO: refactor (remove reliance on firstInCluster) - too complicated.
               return await AdvanceToLocationAsync(initPath, baseDepth);
            }
         } while (await _xmlReader.ReadAsync());

         //end of stream, no xpath found
         return false;
      }


      /// <summary>
      /// Determine if current reader position matches given node pattern
      /// </summary>
      /// <param name="pattern">Definition of a single node</param>
      /// <returns>True if current node matches pattern; false if not.</returns>
      private bool MatchFound(XmlNodeDef pattern)
      {
         if (_xmlReader.NodeType != XmlNodeType.Element) return false;

         if (_xmlReader.Name != pattern.Name) return false;

         //Name matches, let's look for attributes:
         var attribsToMatch = pattern.GetAttributeDict();
         if (!attribsToMatch.Any()) return true;

         var retVal = false;
         if (_xmlReader.HasAttributes)
         {  //locate attributes matching name and value
            while (_xmlReader.MoveToNextAttribute())
            {
               if (attribsToMatch.ContainsKey(_xmlReader.Name))
               {  //attribute name match, remove from the list to match, but only if value matches (or no value in pattern)
                  var valueToMatch = attribsToMatch[_xmlReader.Name];
                  if (valueToMatch == null || valueToMatch.ToString() == _xmlReader.Value)
                  {
                     attribsToMatch.Remove(_xmlReader.Name);
                     if (!attribsToMatch.Any())
                     {  //success! we matched all attributes
                        retVal = true;
                        break;
                     }
                  }
                  else break; //match failed; no chance to run into the same attribute again
               }
            }
         }
         return retVal;
      }


      /// <summary>
      /// Create record from the nodes inside current node.
      /// </summary>
      /// <returns>Record created (or null if no current record, i.e. EOF).</returns>
      private Xrecord GetCurrentXrecord()
      {
         //In spite of checking _xmlReader.EOF before calling this method, a possibility exists that this method is called after the last XML node.
         //This happens if there is a memory stream underneath xmlReader.

         _xmlReader.MoveToContent(); //likely not needed, but added to assure same behavior in Debug and Release (note IsStartElement in Debug.Assert that makes a call to MoveToContent)

         //_xmlReader is expected here at the start element or (rarely) at end of data
         Debug.Assert(_xmlReader.IsStartElement() || _xmlReader.Depth == 0);

         var xrecordItems = new List<Tuple<string, object>>();
         var elemDepth = _xmlReader.Depth;

         //Add record items from inner elements
         var explicitText = AddXrecordItems(xrecordItems, elemDepth, string.Empty);  //this will add items to xrecordItems

         if (explicitText == null) return null; //means no current record, i.e. EOF

         if (_includeExplicitText && !string.IsNullOrWhiteSpace(explicitText))
         {  // add a text placed directly in the record element node (this is not typically expected)
            xrecordItems.Add(Tuple.Create("__explicitText__", explicitText as object));
         }

         return new Xrecord(xrecordItems, _currClstrCnt, _traceBin);
      }


      /// <summary>
      /// Asynchronously create record from the nodes inside current node.
      /// </summary>
      /// <returns>Record created (or null if no current record, i.e. EOF).</returns>
      private async Task<Xrecord> GetCurrentXrecordAsync()
      {
         //In spite of checking _xmlReader.EOF before calling this method, a possibility exists that this method is called after the last XML node.
         //This happens if there is a memory stream underneath xmlReader.

         await _xmlReader.MoveToContentAsync(); //likely not needed, but added to assure same behavior in Debug and Release (note IsStartElement in Debug.Assert that makes a call to MoveToContent)

         //_xmlReader is expected here at the start element or (rarely) at end of data
         Debug.Assert(_xmlReader.IsStartElement() || _xmlReader.Depth == 0);

         var xrecordItems = new List<Tuple<string, object>>();
         var elemDepth = _xmlReader.Depth;

         //Add record items from inner elements
         var explicitText = await AddXrecordItemsAsync(xrecordItems, elemDepth, string.Empty);  //this will add items to xrecordItems

         if (explicitText == null) return null; //means no current record, i.e. EOF

         if (_includeExplicitText && !string.IsNullOrWhiteSpace(explicitText))
         {  // add a text placed directly in the record element node (this is not typically expected)
            xrecordItems.Add(Tuple.Create("__explicitText__", explicitText as object));
         }

         return new Xrecord(xrecordItems, _currClstrCnt, _traceBin);
      }


      /// <summary>
      /// Recursive method to add items to a given element.
      /// </summary>
      /// <param name="itemsSoFar">List of record items to add new items to.</param>
      /// <param name="elemDepth">Depth (level) of the element.</param>
      /// <param name="keySoFar">Name prefix "accumulated" so far, empty except for nested items, i.e. recursive call where higher level item keys are prefixed with a . separator, e.g. Name.Last</param>
      /// <returns>Text (accumulated) of the current node (or null if no current node, i.e. EOF).</returns>
      private string AddXrecordItems(List<Tuple<string, object>> itemsSoFar, int elemDepth, string keySoFar)
      {
         //Add record items from attributes
         if (_includeAttributes) GetAttributes(keySoFar, _addPrefixToAttrKeys).ToList().ForEach(a => itemsSoFar.Add(a));

         _xmlReader.MoveToContent();

         //_xmlReader is expected here at the start element or (rarely) at end of data
         Debug.Assert(_xmlReader.IsStartElement() || _xmlReader.Depth == 0);  //each recursive call must be at the element node (or rarely this method may be called at EOF)

         string retVal = string.Empty; //to be returned (to the higher/outer recursive level)
         string innerVal = null;  //to be obtained from the lower recursive level

         while (_xmlReader.Read() && _xmlReader.Depth > elemDepth)
         {
            string innerKey = keySoFar;

            if (_xmlReader.IsEmptyElement)
            { // special case (e.g. <Empty/>), will not be followed by the end element; add item immediately (but first, add attributes if any)
               var currentKey = CreateKey(keySoFar);
               if (_includeAttributes) GetAttributes(currentKey, _addPrefixToAttrKeys).ToList().ForEach(a => itemsSoFar.Add(a));
               itemsSoFar.Add(Tuple.Create(currentKey, string.Empty as object));
               continue;
            }
            if (_xmlReader.IsStartElement())
            { //recursive call to inner level
               innerKey = CreateKey(innerKey);
               innerVal = AddXrecordItems(itemsSoFar, elemDepth + 1, innerKey);
            }
            if (_xmlReader.NodeType == XmlNodeType.Text)
            { // cumulate text found on current level
               retVal += _xmlReader.Value;
            }
            if (_xmlReader.NodeType == XmlNodeType.EndElement)
            {  // done accumulating, add item
               itemsSoFar.Add(Tuple.Create(innerKey, innerVal as object));
            }
         }

         //It's possible that EOF was not detected until execution of Read in this method above (the reason is that XmlReader based on memory stream
         // can have Read return false, but no EOF until the next attempt to call Read).
         //So, we're returning null to indicate that there is no current record.
         return _xmlReader.EOF ? null : retVal;
      }


      /// <summary>
      /// Recursive method to asynchronously add items to a given element.
      /// </summary>
      /// <param name="itemsSoFar">List of record items to add new items to.</param>
      /// <param name="elemDepth">Depth (level) of the element.</param>
      /// <param name="keySoFar">Name prefix "accumulated" so far, empty except for nested items, i.e. recursive call where higher level item keys are prefixed with a . separator, e.g. Name.Last</param>
      /// <returns>Text (accumulated) of the current node (or null if no current node, i.e. EOF).</returns>
      private async Task<string> AddXrecordItemsAsync(List<Tuple<string, object>> itemsSoFar, int elemDepth, string keySoFar)
      {
         //Add record items from attributes
         if (_includeAttributes) GetAttributes(keySoFar, _addPrefixToAttrKeys).ToList().ForEach(a => itemsSoFar.Add(a));

         await _xmlReader.MoveToContentAsync();

         //_xmlReader is expected here at the start element or (rarely) at end of data
         Debug.Assert(_xmlReader.IsStartElement() || _xmlReader.Depth == 0);  //each recursive call must be at the element node (or rarely this method may be called at EOF)

         string retVal = string.Empty; //to be returned (to the higher/outer recursive level)
         string innerVal = null;  //to be obtained from the lower recursive level

         while (await _xmlReader.ReadAsync() && _xmlReader.Depth > elemDepth)
         {
            string innerKey = keySoFar;

            if (_xmlReader.IsEmptyElement)
            { // special case (e.g. <Empty/>), will not be followed by the end element; add item immediately (but first, add attributes if any)
               var currentKey = CreateKey(keySoFar);
               if (_includeAttributes) GetAttributes(currentKey, _addPrefixToAttrKeys).ToList().ForEach(a => itemsSoFar.Add(a));
               itemsSoFar.Add(Tuple.Create(currentKey, string.Empty as object));
               continue;
            }
            if (_xmlReader.IsStartElement())
            { //recursive call to inner level
               innerKey = CreateKey(innerKey);
               innerVal = await AddXrecordItemsAsync(itemsSoFar, elemDepth + 1, innerKey);
            }
            if (_xmlReader.NodeType == XmlNodeType.Text)
            { // cumulate text found on current level
               retVal += _xmlReader.Value;
            }
            if (_xmlReader.NodeType == XmlNodeType.EndElement)
            {  // done accumulating, add item
               itemsSoFar.Add(Tuple.Create(innerKey, innerVal as object));
            }
         }

         //It's possible that EOF was not detected until execution of Read in this method above (the reason is that XmlReader based on memory stream
         // can have ReadAsync return false, but no EOF until the next attempt to call ReadAsync).
         //So, we're returning null to indicate that there is no current record.
         return _xmlReader.EOF ? null : retVal;
      }


      /// <summary>
      /// Read attributes of the current node and create corresponding items.
      /// </summary>
      /// <param name="keySoFar">The key created from outer nodes.</param>
      /// <param name="prependAtSign">true = prepend name with @; false otherwise.</param>
      /// <returns>A (possibly empty) sequence of items created from current node.</returns>
      private IEnumerable<Tuple<string, object>> GetAttributes(string keySoFar, bool prependAtSign)
      {
         if (_xmlReader.HasAttributes)
         {
            while (_xmlReader.MoveToNextAttribute())
            {
               var attrKey = CreateKey(keySoFar, prependAtSign);  //keys created out of attribute names are prefixed with @ (unless _addPrefixToAttrKeys is false)
               yield return Tuple.Create(attrKey, _xmlReader.Value as object);  //note that XML values (unlike JSON values) are always of string type
            }
         }
         yield break;
      }


      /// <summary>
      /// Determine item key by appending name of the current node (or attribute) to the key built from outer nodes.
      /// </summary>
      /// <param name="keySoFar">The key created from outer nodes.</param>
      /// <param name="prependAtSign">true = prepend name with @ (to be used with attributes), false = just use the name of the current node (or attribute) w/o @ prefix.</param>
      /// <returns>The new key value</returns>
      private string CreateKey(string keySoFar, bool prependAtSign)
      {
         var newKeySegment = prependAtSign ? $"@{_xmlReader.Name}" : _xmlReader.Name;
         return string.IsNullOrEmpty(keySoFar) ? newKeySegment : $"{keySoFar}.{newKeySegment}";
      }
      private string CreateKey(string keySoFar) => CreateKey(keySoFar, false);  //this overload applies to element nodes (also to attributes if _addPrefixToAttrKeys is false)
   }
}
