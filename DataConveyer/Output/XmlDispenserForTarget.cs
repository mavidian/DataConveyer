//XmlDispenserForTarget.cs
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

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   ///  XML writer to a single target.
   /// </summary>
   internal class XmlDispenserForTarget : LineDispenserForTarget
   {
      private readonly XmlWriter _xmlWriter;

      //Discrete settings determined by the ctor:
      private readonly XmlNodePath _collNodePath, _clstrNodePath, _recNodePath;
      private readonly HashSet<string> _attributeFields;  //fields to become attributes of record node (not inner nodes) 
      private readonly bool _observeClusters;  //true if non-empty ClusterNode setting is present
      private readonly int _clusterDepth;  //number of nested nodes constituting clstrNodePath

      private int _currClstrNo;  // 1-based last cluster number (0=before 1st cluster)
      private bool _atStart;

      /// <summary>
      /// Creates an instance of an XML dispenser for a single target.
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="targetNo">1 based target number.</param>
      /// <param name="settings"></param>
      /// <param name="outputIsAsync"></param>
      internal XmlDispenserForTarget(TextWriter writer, int targetNo, string settings, bool outputIsAsync) : base(writer, targetNo)
      //Note that writer is passed to base class, even though it is not used there (all relevant methods are overridden, so null could've been passed instead).
      //This is because base.Dispose (called by Dispose in this class) disposes the writer (it is unclear whether XmlWriter.Dispose disposes underlying writer).
      {

         var settingDict = settings?.SplitPairsToListOfTuples()?.ToDictionary(t => t.Item1, t => t.Item2);

         // Discrete settings to unpack:
         //    CollectionNode   - "xpath" defining the collection of clusters/records (may be null/empty, in which case output will contain XML fragment where each root constitutes record or cluster).
         //    ClusterNode      - "xpath" defining clusters node within collection node (null/empty means record nodes are directly inside collection node).
         //    RecordNode       - "xpath" defining record node within cluster node (or collection node if cluster node is empty). RecordNode is mandatory, if absent "__record__" is assumed.
         //    AttributeFields  - a semicolon-separated list of field names (item keys) to be projected into XML as attributes of the record node (and not inner nodes).
         //    IndentChars      - string to use when indenting, e.g. "\t" or "  "; allows "pretty-print" XML output; when absent, no indenting takes place.
         //    NewLineChars     - allows "pretty-print" XML output
         // "xpath" is always relative (no need for ./), each of the nodes is separated by /.
         _collNodePath = new XmlNodePath(settingDict.GetStringSetting("CollectionNode"));
         _clstrNodePath = new XmlNodePath(settingDict.GetStringSetting("ClusterNode"));
         _recNodePath = new XmlNodePath(settingDict.GetStringSetting("RecordNode"));
         if (_recNodePath.IsEmpty) _recNodePath = new XmlNodePath("__record__");  //RecordNode is mandatory; if absent in config, __record__ is assumed
         var attrFlds = settingDict.GetStringSetting("AttributeFields")?.ToListOfStrings(';');
         _attributeFields = attrFlds ==  null ? new HashSet<string>() : new HashSet<string>(attrFlds);
         var indentChars = settingDict.GetStringSetting("IndentChars");
         var newLineChars = settingDict.GetStringSetting("NewLineChars");
         _observeClusters = !_clstrNodePath.IsEmpty;
         _clusterDepth = _observeClusters ? _clstrNodePath.NodeDefs.Count : 0;

         var writerSettings = new XmlWriterSettings() { ConformanceLevel = _collNodePath.IsEmpty ? ConformanceLevel.Fragment : ConformanceLevel.Document};
         if (indentChars != null)
         {
            writerSettings.Indent = true;
            writerSettings.IndentChars = indentChars;
         }
         if (newLineChars != null) writerSettings.NewLineChars = newLineChars;

         writerSettings.Async = outputIsAsync;

         _xmlWriter = XmlWriter.Create(writer, writerSettings);

         _currClstrNo = 0; //will stay at 0 (undetermined) unless ClusterNode defined
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
            _atStart = false;  //not thread-safe, but output is single-threaded
            InitiateDispensing(line.ClstrNo);
         }

         if (_observeClusters && line.ClstrNo != _currClstrNo)  //new cluster
         {  //Close _clstrNodePath and start a new one
            Debug.Assert(_clstrNodePath.NodeDefs.Any());
            WriteCloseNodes(_clstrNodePath.NodeDefs.Count);
            WriteStartNodes(_clstrNodePath);
            _currClstrNo = line.ClstrNo;
         }

         WriteXrecord(_recNodePath, line); //write record contents
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
            _atStart = false;  //not thread-safe, but output is single-threaded
            await InitiateDispensingAsync(line.ClstrNo);
         }

         if (_observeClusters && line.ClstrNo != _currClstrNo)  //new cluster
         {  //Close _clstrNodePath and start a new one
            Debug.Assert(_clstrNodePath.NodeDefs.Any());
            await WriteCloseNodesAsync(_clstrNodePath.NodeDefs.Count);
            await WriteStartNodesAsync(_clstrNodePath);
            _currClstrNo = line.ClstrNo;
         }

         await WriteXrecordAsync(_recNodePath, line); //write record contents
      }


      /// <summary>
      /// Write closing nodes at end of data.
      /// </summary>
      internal override void ConcludeDispensing()
      {
         //This method is intended to be called by LineDispenser (owner) upon receiving EOD mark. At that point, we should have
         // cluster and collection nodes open - let's close them (we're closing them explicitly even though XmlWriter could do it upon closing).
         if (_atStart) return;  //unlikely, but possible, e.g. 2nd target never directed to 
         WriteCloseNodes(_clusterDepth + (_collNodePath.IsEmpty ? 0 : _collNodePath.NodeDefs.Count));
         _xmlWriter.Flush();
      }


      /// <summary>
      /// Asynchronously write closing nodes at end of data.
      /// </summary>
      /// <returns></returns>
      internal override async Task ConcludeDispensingAsync()
      {
         //This method is intended to be called by LineDispenser (owner) upon receiving EOD mark. At that point, we should have
         // cluster and collection nodes open - let's close them (we're closing them explicitly even though XmlWriter could do it upon closing).
         if (_atStart) return;  //unlikely, but possible, e.g. 2nd target never directed to 
         await WriteCloseNodesAsync(_clusterDepth + (_collNodePath.IsEmpty ? 0 : _collNodePath.NodeDefs.Count));
         await _xmlWriter.FlushAsync();
      }



      /// <summary>
      /// Dispose underlying XML writer.
      /// </summary>
      public override void Dispose()
      {
         _xmlWriter.Dispose();
         base.Dispose();
      }


      /// <summary>
      /// Write nodes at the very beginning of the XML document
      /// </summary>
      /// <param name="firstClstrNo"></param>
      private void InitiateDispensing(int firstClstrNo)
      {
         if (!_collNodePath.IsEmpty)
         {
            _xmlWriter.WriteStartDocument();
            WriteStartNodes(_collNodePath);
         }
         if (_observeClusters)
         {
            WriteStartNodes(_clstrNodePath);
            _currClstrNo = firstClstrNo;
         }
      }


      /// <summary>
      /// Asynchronously write nodes at the very beginning of the XML document
      /// </summary>
      /// <param name="firstClstrNo"></param>
      /// <returns></returns>
      private async Task InitiateDispensingAsync(int firstClstrNo)
      {
         if (!_collNodePath.IsEmpty)
         {
            await _xmlWriter.WriteStartDocumentAsync();
            await WriteStartNodesAsync(_collNodePath);
         }
         if (_observeClusters)
         {
            await WriteStartNodesAsync(_clstrNodePath);
            _currClstrNo = firstClstrNo;
         }
      }


      /// <summary>
      /// Write a series of starting nodes based on the path given
      /// </summary>
      /// <param name="nodesToWrite"></param>
      private void WriteStartNodes(XmlNodePath nodesToWrite)
      {
         //note that XmlWriter remembers all nodes that were started, so there is no need to remember them
         Debug.Assert(!nodesToWrite.IsEmpty);
         _ = nodesToWrite.NodeDefs.Skip(1).Any() ? new XmlNodePath(nodesToWrite.NodeDefs.Skip(1))  //the remaining nodes to write during recursive calls
                                                 : null;  //end of recursion

         WriteStartNode(nodesToWrite.NodeDefs[0]);  //head

         if (nodesToWrite.NodeDefs.Skip(1).Any())  //tail
         {  //tail exists; inner node(s) are written using recursion
            WriteStartNodes(new XmlNodePath(nodesToWrite.NodeDefs.Skip(1)));
         }
      }


      /// <summary>
      /// Asynchronously write a series of starting nodes based on the path given
      /// </summary>
      /// <param name="nodesToWrite"></param>
      /// <returns></returns>
      private async Task WriteStartNodesAsync(XmlNodePath nodesToWrite)
      {
         //note that XmlWriter remembers all nodes that were started, so there is no need to remember them
         Debug.Assert(!nodesToWrite.IsEmpty);
         _ = nodesToWrite.NodeDefs.Skip(1).Any() ? new XmlNodePath(nodesToWrite.NodeDefs.Skip(1))  //the remaining nodes to write during recursive calls
                                                 : null;  //end of recursion

         await WriteStartNodeAsync(nodesToWrite.NodeDefs[0]);  //head

         if (nodesToWrite.NodeDefs.Skip(1).Any())  //tail
         {  //tail exists; inner node(s) are written using recursion
            await WriteStartNodesAsync(new XmlNodePath(nodesToWrite.NodeDefs.Skip(1)));
         }
      }


      /// <summary>
      /// Write a single starting node along with its attributes
      /// </summary>
      /// <param name="nodeToWrite">Definition of the node to write</param>
      private void WriteStartNode(XmlNodeDef nodeToWrite)
      {
         _xmlWriter.WriteStartElement(nodeToWrite.Name);
         foreach (var attr in nodeToWrite.GetAttributes())
         {
            _xmlWriter.WriteAttributeString(attr.Item1, attr.Item2?.ToString());
         }
      }


      /// <summary>
      /// Asynchronously write a single starting node along with its attributes
      /// </summary>
      /// <param name="nodeToWrite">Definition of the node to write</param>
      /// <returns></returns>
      private async Task WriteStartNodeAsync(XmlNodeDef nodeToWrite)
      {
         await _xmlWriter.WriteStartElementAsync(null, nodeToWrite.Name, null);
         foreach (var attr in nodeToWrite.GetAttributes())
         {
            await _xmlWriter.WriteAttributeStringAsync(null, attr.Item1, null, attr.Item2?.ToString());
         }
      }


      /// <summary>
      /// Write a series of closing nodes.
      /// </summary>
      /// <param name="howMany">Number of consecutive nodes (levels) to close.</param>
      private void WriteCloseNodes(int howMany)
      {
         while (howMany-- > 0) _xmlWriter.WriteEndElement();
      }


      /// <summary>
      /// Asynchronously write a series of closing nodes.
      /// </summary>
      /// <param name="howMany">Number of consecutive nodes (levels) to close.</param>
      /// <returns></returns>
      private async Task WriteCloseNodesAsync(int howMany)
      {
         while (howMany-- > 0) await _xmlWriter.WriteEndElementAsync();
      }


      /// <summary>
      /// Send items of the current record to XML output.
      /// </summary>
      /// <param name="xrecNodePath"></param>
      /// <param name="line"></param>
      private void WriteXrecord(XmlNodePath xrecNodePath, ExternalLine line)
      {
         Debug.Assert(line.GetType() == typeof(Xrecord));

         var head = xrecNodePath.NodeDefs[0];
         var tail = xrecNodePath.NodeDefs.Skip(1).Any() ? new XmlNodePath(xrecNodePath.NodeDefs.Skip(1))  //XmlNodePath representing the remaining node patterns
                                                        : null;  //end of recursion

         _xmlWriter.WriteStartElement(head.Name);  //start element

         var attrsToWrite = head.GetAttributes();

         if (tail == null)  //leaf level (end of recursion)
         {
            //Don't write records right away as some items may need to be written as attributes (and not inner nodes)
            // Instead, collect all attributes and inner nodes to write.
            //Items to be written as attributes are those that start with @ or are listed in AttributeFields part of XmlJsonOutputSettings.
            var innerNodesToWrite = new List<Tuple<string, object>>();
            foreach (var item in line.Items)
            {
               if (item.Item1[0] == '@')
               {  //attribute
                  attrsToWrite.Add(Tuple.Create(item.Item1.Substring(1),item.Item2));
               }
               else if (_attributeFields.Contains(item.Item1))
               {  //attribute
                  attrsToWrite.Add(item);
               }
               else
               { //inner node
                  innerNodesToWrite.Add(item);
               }
            }
            attrsToWrite.ForEach(a => _xmlWriter.WriteAttributeString(a.Item1, a.Item2?.ToString()));  //attributes (note that ToString() for string is just a reference to itself, so no perfromance penalty)
            innerNodesToWrite.ForEach(n => _xmlWriter.WriteElementString(n.Item1, n.Item2?.ToString())); //inner nodes
         }
         else
         { //we're not at the leaf level yet, write the attributes and recurse
            attrsToWrite.ForEach(a => _xmlWriter.WriteAttributeString(a.Item1, a.Item2?.ToString()));  //attributes
            WriteXrecord(tail, line);
         }
         _xmlWriter.WriteEndElement();  //end element (note that it will skip full end tag in case of no inner nodes -WriteFullEndElement can be used to avoid this)
      }


      /// <summary>
      /// Asynchronously send items of the current record to XML output.
      /// </summary>
      /// <param name="xrecNodePath"></param>
      /// <param name="line"></param>
      /// <returns></returns>
      private async Task WriteXrecordAsync(XmlNodePath xrecNodePath, ExternalLine line)
      {
         Debug.Assert(line.GetType() == typeof(Xrecord));

         var head = xrecNodePath.NodeDefs[0];
         var tail = xrecNodePath.NodeDefs.Skip(1).Any() ? new XmlNodePath(xrecNodePath.NodeDefs.Skip(1))  //XmlNodePath representing the remaining node patterns
                                                        : null;  //end of recursion

         await _xmlWriter.WriteStartElementAsync(null, head.Name, null);  //start element

         var attrsToWrite = head.GetAttributes();

         if (tail == null)  //leaf level (end of recursion)
         {
            //Don't write records right away as some items may need to be written as attributes (and not inner nodes)
            // Instead, collect all attributes and inner nodes to write.
            //Items to be written as attributes are those that start with @ or are listed in AttributeFields part of XmlJsonOutputSettings.
            var innerNodesToWrite = new List<Tuple<string, object>>();
            foreach (var item in line.Items)
            {
               if (item.Item1[0] == '@')
               {  //attribute
                  attrsToWrite.Add(Tuple.Create(item.Item1.Substring(1), item.Item2));
               }
               else if (_attributeFields.Contains(item.Item1))
               {  //attribute
                  attrsToWrite.Add(item);
               }
               else
               { //inner node
                  innerNodesToWrite.Add(item);
               }
            }
            attrsToWrite.ForEach(async a => await _xmlWriter.WriteAttributeStringAsync(null, a.Item1, null, a.Item2?.ToString()));  //attributes (note that ToString() for string is just a reference to itself, so no perfromance penalty)
            innerNodesToWrite.ForEach(async n => await _xmlWriter.WriteElementStringAsync(null, n.Item1, null, n.Item2?.ToString())); //inner nodes
         }
         else
         { //we're not at the leaf level yet, write the attributes and recurse
            attrsToWrite.ForEach(async a => await _xmlWriter.WriteAttributeStringAsync(null, a.Item1, null,  a.Item2?.ToString()));  //attributes
            await WriteXrecordAsync(tail, line);
         }
         await _xmlWriter.WriteEndElementAsync();  //end element (note that it will skip full end tag in case of no inner nodes (WriteFullEndElementAsync can be used to avoid this)
      }

   }
}
