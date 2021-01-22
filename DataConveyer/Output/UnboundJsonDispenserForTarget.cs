//UnboundJsonDispenserForTarget.cs
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
      private readonly bool _produceStandaloneObjects; // if present, the output contains concatenated JSON objects (object=record) or arrays (array=cluster) - technically not a valid JSON, but commonly used; if absent (default), single array on output.
      private readonly bool _produceClusters; // if present, all objects (records) are enclosed in arrays, where each array represents a cluster; if absent (default), clusters are ignored.
      private readonly bool _skipColumnPresorting; // if present, no pre-sorting of keys (column names) occurs - performance feature, but JSON hierarchy may be off; if absent (default), columns are groupped by segments (in order of appearance.

      private int _currClstrNo;  // 1-based last cluster number (0=before 1st cluster)
      private bool _atStart;

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
         //    ProduceStandaloneObjects - if present, multiple JSON objects are produced on output (technically not a valid JSON); if absent, a JSON array is produced (each element is an object, i.e. a record)
         //    ProduceClusters          - if present, all objects (records) are enclosed in arrays, where each array represents a cluster; if absent (default), clusters are ignored.
         //    SkipColumnPresorting     - if present, the keys (field names) are processed in order appearing (better perfrormance); if absent, keys are groupped by segments to assure proper JSON hierarchy nesting 
         //    IndentChars              - string to use when indenting, e.g. "\t" or "  "; allows "pretty-print" JSON output; when absent, no indenting takes place. Due to JSON.NET library limitation, the string must consist of identical characters.

         _produceStandaloneObjects = settingDict?.ContainsKey("ProduceStandaloneObjects") ?? false;
         _produceClusters = settingDict?.ContainsKey("ProduceClusters") ?? false;
         _skipColumnPresorting = settingDict?.ContainsKey("SkipColumnPresorting") ?? false;

         var indentChars = settingDict.GetStringSetting("IndentChars");

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
            InitiateDispensing(line.ClstrNo);
         }

         if (_produceClusters && line.ClstrNo != _currClstrNo)
         {  //new cluster
            if (!_atStart)
            {
               _jsonWriter.WriteEndArray();
               if (_produceStandaloneObjects && _jsonWriter.Formatting == Formatting.Indented) _underlyingWriter.WriteLine();  // an extra new line to help in "pretty printing"
            }
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
            await InitiateDispensingAsync(line.ClstrNo);
         }

         if (_produceClusters && line.ClstrNo != _currClstrNo)
         {  //new cluster
            if (!_atStart)
            {
               await _jsonWriter.WriteEndArrayAsync();
               if (_produceStandaloneObjects && _jsonWriter.Formatting == Formatting.Indented) _underlyingWriter.WriteLine();  // an extra new line to help in "pretty printing"
            }
            await _jsonWriter.WriteStartArrayAsync();
            _currClstrNo = line.ClstrNo;
         }

         await WriteXrecordAsync(line); //write record contents

         _atStart = false;
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
         if (!_produceStandaloneObjects) _jsonWriter.WriteEndArray();
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
         if (_atStart) return;  //unlikely, but possible, e.g. 2nd target never directed to
         if (_produceClusters) _jsonWriter.WriteEndArray();
         if (!_produceStandaloneObjects) _jsonWriter.WriteEndArray();
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
         if (!_produceStandaloneObjects) _jsonWriter.WriteStartArray();
      }


      /// <summary>
      /// Asynchronously write nodes at the very beginning of the JSON document.
      /// </summary>
      /// <param name="firstClstrNo"></param>
      /// <returns></returns>
      private async Task InitiateDispensingAsync(int firstClstrNo)
      {
         if (!_produceStandaloneObjects) await _jsonWriter.WriteStartArrayAsync();
      }


      /// <summary>
      /// Send items of the current record to JSON output.
      /// </summary>
      /// <param name="line"></param>
      private void WriteXrecord(ExternalLine line)
      {
         Debug.Assert(line.GetType() == typeof(Xrecord));

         if (_produceStandaloneObjects && _jsonWriter.Formatting == Formatting.Indented && !_atStart && !_produceClusters)
         {  //we're starting a new JSON document here (technically not a valid JSON)
            //add an extra new line to help in "pretty printing"
            _underlyingWriter.WriteLine();
         }
         _jsonWriter.WriteStartObject();

         WriteItems(_skipColumnPresorting ? line.Items : PresortItems(line.Items));

         _jsonWriter.WriteEndObject();
      }


      /// <summary>
      /// Sort data in a way to facilitate creation of nested JSON hierarchy.
      /// All top key segments are groupped together (in order of first appearance), then 2nd key segments are groupped together, etc.
      /// </summary>
      /// <param name="items">A sequence of key value pairs where key is a compound column name.</param>
      /// <returns>A sequence of sorted key value pairs.</returns>
      internal static IEnumerable<Tuple<string, object>> PresortItems(IEnumerable<Tuple<string, object>> items)
      //TODO: make private (and refactor unit tests to use private accessor intead of direct call)
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
      private static IEnumerable<Tuple<string[], Tuple<string, object>>> SortWrappedData(IEnumerable<Tuple<string[], Tuple<string, object>>> wrappedItems)
      {  // WrappedItem: Item1 = Wrapper, Item2 = Item
         var groupedItems = wrappedItems.GroupBy(wi => wi.Item1.Length == 0 ? null : wi.Item1[0]);  //wrappedItems: Item1 = Wrapper, Item2 = Item
         if (groupedItems == null) yield break;
         // Notice that segments are groupped in order of appearance. So, array elements are not sorted by index.
         foreach (var group in groupedItems)
         {
            // Remove top (grouped by) segment from the Wrapper when recursing
            var innerGroup = group.Select(wi => Tuple.Create(wi.Item1.Skip(1).ToArray(), wi.Item2));
            if (innerGroup.Count() == 1 && !innerGroup.First().Item1.Any()) yield return group.First();  // end of recursion (note that keys are unique, so only a single item exists w/o inner items)
            else foreach (var wi in SortWrappedData(innerGroup)) yield return wi;
         }
      }
      /// <summary>
      /// Send a sequence of key-value pairs as a JSON hierarchy to JSON output.
      /// </summary>
      /// <param name="items">Key-value pairs where Key is the path to a JSON element. Represents a single record.</param>
      private void WriteItems(IEnumerable<Tuple<string,object>> items)
      {  // Item: Item1 = Key, Item2 = Value
         // The Key is divided into segments, e.g. the segements of Arr[3].Obj.InnArr[2] are Arr, 3, Obj, InnArr and 2.
         // Each segment is represented by Label or Counter; which in turn corresponds to JSON object, array element or value based on the segment that follows it, like so:
         //   Arr    - Label   - JSON array (as it is followed by a Counter)
         //   3      - Counter - JSON object (as it is followed by a Label)
         //   Obj    - Label   - JSON object (as it is followed by a Label)
         //   InnArr - Label   - JSON array (as it is followed by a Counter)
         //   2      - Counter - JSON value (as it is the last element)
         var segmentsSoFar = new Stack<LorC>(); // reflect current/previous nesting on JSON output
         segmentsSoFar.Push(new LorC("dummy~!`'&^%$???")); // starting point ("previously output path"); will get removed (as long as no match with actual key)
         foreach (var item in items)
         {
            var currSegments = SplitColumnName(item.Item1);
            var unchangedSegmentsCount = currSegments.Zip(segmentsSoFar.Reverse(), (f, s) => Tuple.Create(f,s)).TakeWhile(t => t.Item1.Equals(t.Item2)).Count();
            while (segmentsSoFar.Skip(unchangedSegmentsCount + 1).Any())  // equivalent to while (keySegmentsSoFar.Count > unchangedSegmentsCount + 1), but more efficient
            {
               WriteEndBracketToJson(segmentsSoFar.Pop());
            }
            segmentsSoFar.Pop();
            // Here, segmentsSoFar contains beginning segments that are the same as in prior item.
            bool valueFlag = true;  // otherwise, either an object or array
            foreach (var segment in currSegments.Skip(unchangedSegmentsCount))
            {
               if (segment.IsLabel)
               {
                  if (!valueFlag)
                     _jsonWriter.WriteStartObject();
                  _jsonWriter.WritePropertyName(segment.Label);
               }
               else //IsCounter
               {
                  if (!valueFlag)
                     _jsonWriter.WriteStartArray();
               }
               valueFlag = false;
               segmentsSoFar.Push(segment);
            }
            _jsonWriter.WriteValue(item.Item2);
         }
         // Close all "pending" objects and arrays, but not the value (marked above by 'valueFlag') 
         while (segmentsSoFar.Skip(1).Any())  // equivalent to while (keySegmentsSoFar.Count > 1)
         {
            WriteEndBracketToJson(segmentsSoFar.Pop());
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
            yield return item.Last() == ']'
                        ? new LorC(int.Parse(item.TrimEnd(']')))  // counter
                        : new LorC(item);  // label
         }
      }


      /// <summary>
      /// Write end of either object or array to JSON output.
      /// </summary>
      /// <param name="segment">Label or Counter object.</param>
      private void WriteEndBracketToJson(LorC segment)
      {
         if (segment.IsLabel)
            _jsonWriter.WriteEndObject();
         else
            _jsonWriter.WriteEndArray();
      }


      /// <summary>
      /// Asynchronously send items of the current record to JSON output.
      /// </summary>
      /// <param name="line"></param>
      /// <returns></returns>
      private async Task WriteXrecordAsync(ExternalLine line)
      {
         Debug.Assert(line.GetType() == typeof(Xrecord));

         if (_produceStandaloneObjects && _jsonWriter.Formatting == Formatting.Indented && !_atStart && !_produceClusters)
         {  //we're starting a new JSON document here (technically not a valid JSON)
            //add an extra new line to help in "pretty printing"
            await _underlyingWriter.WriteLineAsync();
         }

         await _jsonWriter.WriteStartObjectAsync();

         await WriteItemsAsync(_skipColumnPresorting ? line.Items : PresortItems(line.Items));

         await _jsonWriter.WriteEndObjectAsync();
      }


      /// <summary>
      /// Asynchronously send a sequence of key-value pairs as a JSON hierarchy to JSON output.
      /// </summary>
      /// <param name="items">Key-value pairs where Key is the path to a JSON element. Represents a single record.</param>
      private async Task WriteItemsAsync(IEnumerable<Tuple<string, object>> items)
      {  // Item: Item1 = Key, Item2 = Value
         var prevKey = new Stack<LorC>();
         prevKey.Push(new LorC("dummy")); //will get removed
         foreach (var item in items)
         {
            var segments = SplitColumnName(item.Item1);
            var unchangedSegmentsCount = segments.Zip(prevKey.Reverse(), (f, s) => Tuple.Create(f, s)).TakeWhile(t => t.Item1.Equals(t.Item2)).Count();
            while (prevKey.Count > unchangedSegmentsCount + 1)
            {
               var segment = prevKey.Pop();
               if (segment.IsLabel) await _jsonWriter.WriteEndObjectAsync(); else await _jsonWriter.WriteEndArrayAsync();
            }
            prevKey.Pop();

            bool first = true;
            foreach (var segment in segments.Skip(unchangedSegmentsCount))
            {
               if (segment.IsLabel)
               {
                  if (!first) await _jsonWriter.WriteStartObjectAsync();
                  await _jsonWriter.WritePropertyNameAsync(segment.Label);
               }
               else //IsCounter
               {
                  if (!first) await _jsonWriter.WriteStartArrayAsync();
               }
               first = false;
               prevKey.Push(segment);
            }
            await _jsonWriter.WriteValueAsync(item.Item2);
         }
      }


   }
}
