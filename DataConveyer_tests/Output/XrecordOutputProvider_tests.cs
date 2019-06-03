//XrecordOutputProvider_tests.cs
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


using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace DataConveyer_tests.Output
{

   [TestClass]
   public class XrecordOutputProvider_tests
   {
      OrchestratorConfig _config;

      int _inPtr = 0;
      List<Tuple<ExternalLine, int>> _inLines;

      private Tuple<ExternalLine, int> _inLine(IGlobalCache gc)
      {
         if (_inPtr >= _inLines.Count) return null;
         return _inLines[_inPtr++];
      }

      List<ExternalLine> _resultingLines;  //container of the test results

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Susan,@pNUM=323";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=\"Mary,Ann\",@pNUM=423";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=523";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=623";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Joan,@pNUM=723";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Jane,@pNUM=823";
         yield return "@pABCD_ID=,@pNAME=Cindy,@pNUM=923,@pRECTYPE=ABCD";
         yield return "EOF";
      }


      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();

         _inLines = _intakeLines().Select(l => l.ToExternalTuple()).ToList();
         _resultingLines = new List<ExternalLine>();
      }


      [TestMethod]
      public void produceExtLineOfTypeXrecord_SimpleSettings_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.XML;
         _config.SetOutputConsumer((IEnumerable<Tuple<string, object>> r) => { _resultingLines.Add(r.ToExternalLine()); });  //note that type of lambda argument needs to be defined (disambiguated)

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Count.Should().Be(11);  //10 records + EOD mark

         var xRec = _resultingLines[0];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(3);
         var xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("XYZ");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Mary");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be("123");

         xRec = _resultingLines[2];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be("XYZ00883");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Susan");
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be("323");

         xRec = _resultingLines[3];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be("XYZ00883");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Mary,Ann");   //commas OK
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be("423");

         xRec = _resultingLines[6];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be("XYZ00883");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Joan");
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be("723");

         xRec = _resultingLines[8];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be(string.Empty);
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Cindy");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be("923");
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");

         xRec = _resultingLines[9];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(1);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("EOF");
         xItm.Item2.Should().BeNull();  //note that this is null even for XML - this is b/c EOF field exists - it was parsed on intake as string with a null value  (IOW, it is not a void item)

         xRec = _resultingLines[10];
         xRec.Should().BeNull();  //EOD mark
      }


      [TestMethod]
      public void produceExtLineOfTypeXrecord_SomeIntCalcs_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (r, prevR, rCnt) => { return (string)r["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = r =>
         {
            if (r.ContainsKey("NUM")) r["NUM"] = (int)r["NUM"] + 4;
            return r;  // not the best practice to returne smae (mutated) record instance, but good test.
         };
         _config.OutputDataKind = KindOfTextData.JSON;  // note that in case of XML, all fields passed to OutputConsumer (incl. NUM) would've been converted to strings
         _config.SetOutputConsumer((IEnumerable<Tuple<string, object>> r) => { _resultingLines.Add(r.ToExternalLine()); });  //note that type of lambda argument needs to be defined (disambiguated)

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(10);

          _resultingLines.Count.Should().Be(11);  //10 records + EOD mark

         var xRec = _resultingLines[0];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(3);
         var xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("XYZ");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Mary");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be(127);  //note that NUM is of int type

         xRec = _resultingLines[2];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be("XYZ00883");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Susan");
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be(327);  //note that NUM is of int type

         xRec = _resultingLines[3];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be("XYZ00883");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Mary,Ann");   //commas OK
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be(427);  //note that NUM is of int type

         xRec = _resultingLines[6];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be("XYZ00883");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Joan");
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be(727);  //note that NUM is of int type

         xRec = _resultingLines[8];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(4);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("ABCD_ID");
         xItm.Item2.Should().Be(string.Empty);
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Cindy");
         xItm = xRec.Items[2];
         xItm.Item1.Should().Be("NUM");
         xItm.Item2.Should().Be(927);  //note that NUM is of int type
         xItm = xRec.Items[3];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");

         xRec = _resultingLines[9];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(1);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("EOF");
         xItm.Item2.Should().BeNull();

         xRec = _resultingLines[10];
         xRec.Should().BeNull();  //EOD mark
      }


      [TestMethod]
      public void produceExtLineOfTypeXrecord_OutputFieldsDefined_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.JSON;  // note that in case of XML, all fields passed to OutputConsumer would've been converted to strings (and void items would've been converted to "" instead of nulls)
         _config.OutputFields = "RECTYPE,NAME";
         _config.SetOutputConsumer((IEnumerable<Tuple<string, object>> r) => { _resultingLines.Add(r.ToExternalLine()); });  //note that type of lambda argument needs to be defined (disambiguated)

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Count.Should().Be(11);  //10 records + EOD mark

         var xRec = _resultingLines[0];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(2);
         var xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("XYZ");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Mary");

         xRec = _resultingLines[2];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(2);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Susan");

         xRec = _resultingLines[3];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(2);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];

         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Mary,Ann");

         xRec = _resultingLines[6];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(2);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Joan");

         xRec = _resultingLines[8];
         xRec.Should().BeOfType<Xrecord>();
         xRec.Items.Count.Should().Be(2);
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");  //note that OutputFields drives order, which is why RECTYPE comes before NAME
         xItm.Item2.Should().Be("ABCD");
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().Be("Cindy");

         xRec = _resultingLines[9];
         xRec.Should().BeOfType<Xrecord>();
         xItm = xRec.Items[0];
         xItm.Item1.Should().Be("RECTYPE");  //fields not present result in void items, which have null values for JSON (but would've been "" for XML)
         xItm.Item2.Should().BeNull();
         xItm = xRec.Items[1];
         xItm.Item1.Should().Be("NAME");
         xItm.Item2.Should().BeNull();

         xRec = _resultingLines[10];
         xRec.Should().BeNull();  //EOD mark
      }

   }
}