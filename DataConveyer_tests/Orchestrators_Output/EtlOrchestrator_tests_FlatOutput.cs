//EtlOrchestrator_tests_FlatOutput.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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

namespace DataConveyer_tests.Orchestrators_Output
{
   /// <summary>
   /// These are in fact integration tests (of the entire pipeline)
   /// </summary>
   [TestClass]
   public class EtlOrchestrator_tests_FlatOutput
   {
      OrchestratorConfig _config;

      int _inPtr = 0;
      List<Tuple<ExternalLine, int>> _inLines;

      private Tuple<ExternalLine, int> _inLine(IGlobalCache gc)
      {
         if (_inPtr >= _inLines.Count) return null;
         return _inLines[_inPtr++];
      }

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=\"Mary, Ann\",@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Susan,@pNUM=323";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=523";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=623";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME= Joan  ,@pNUM=723";  //note Joan has 1 leading and 2 trailing spaces
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Jane,@pNUM=823";
         yield return "@pABCD_ID=XYZ00883,@pNAME=Cindy,@pNUM=923,@pRECTYPE=ABCD";
         yield return "EOF";
      }


      //Result of the tests are held here:
      List<string> _resultingLines;  //container of the test results

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();

         //prepare extraction of the results from the pipeline
         _resultingLines = new List<string>();
         _inLines = _intakeLines().Select(l => l.ToExternalTuple()).ToList();
      }


      [TestMethod]
      public void processPipeline_SimpleSettings_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Count.Should().Be(11);  //10 + null line at end

         _resultingLines[0].Should().Be("XYZ         Mary, Ann           123       ");   //no quotes even if value has embedded commas/quotes
         _resultingLines[2].Should().Be("ABCD        Susan               323       ");
         _resultingLines[3].Should().Be("ABCD        Mary                423       ");
         _resultingLines[4].Should().Be("XYZ         Mary                523       ");
         _resultingLines[6].Should().Be("ABCD         Joan               723       ");   //Joan has leading space
         _resultingLines[8].Should().Be("ABCD        Cindy               923       ");
         _resultingLines[9].Should().Be("                                          ");
         _resultingLines[10].Should().BeNull();
      }

      [TestMethod]
      public void processPipeline_HeaderRowAlsoExcludeExtra_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM,ABCD_ID|8";
         _config.HeadersInFirstOutputRow = true;
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(11);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE     NAME                NUM       ABCD_ID");
         _resultingLines[1].Should().Be("XYZ         Mary, Ann           123");
         _resultingLines[3].Should().Be("ABCD        Susan               323       XYZ00883");
         _resultingLines[4].Should().Be("ABCD        Mary                423       XYZ00883");
         _resultingLines[5].Should().Be("XYZ         Mary                523");
         _resultingLines[7].Should().Be("ABCD         Joan               723       XYZ00883");   //Joan has leading space
         _resultingLines[9].Should().Be("ABCD        Cindy               923       XYZ00883");
         _resultingLines[10].Should().Be(string.Empty);
         _resultingLines[11].Should().BeNull();
      }

      [TestMethod]
      public void processPipeline_FormatAlsoOutputKeyPrefix_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, "0000000000") : new ItemDef(ItemType.String, null);  //NUM Int - 10 digits, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputKeyPrefix = "*";
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM,ABCD_ID|8";
         _config.HeadersInFirstOutputRow = true;
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(11);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE     NAME                NUM       ABCD_ID");  //OutputKeyPrefix is ignored in case of flat data
         _resultingLines[1].Should().Be("XYZ         Mary, Ann           0000000123");
         _resultingLines[3].Should().Be("ABCD        Susan               0000000323XYZ00883");
         _resultingLines[4].Should().Be("ABCD        Mary                0000000423XYZ00883");
         _resultingLines[5].Should().Be("XYZ         Mary                0000000523");
         _resultingLines[7].Should().Be("ABCD         Joan               0000000723XYZ00883");   //Joan has leading space (and 2 trailing spaces)
         _resultingLines[9].Should().Be("ABCD        Cindy               0000000923XYZ00883");
         _resultingLines[10].Should().Be(string.Empty);
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_QuotationModeAlways_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.QuotationMode = QuotationMode.Always;
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM,ABCD_ID|8";
         _config.HeadersInFirstOutputRow = true;
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(11);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("\"RECTYPE\"   \"NAME\"              \"NUM\"     \"ABCD_I\"");  //note that ending quote is always provided, which may cut off traling characters from contents
         _resultingLines[1].Should().Be("\"XYZ\"       \"Mary, Ann\"         \"123\"     \"\"");        //note the empty last field becomes "" when surrounded with quotes on output
         _resultingLines[3].Should().Be("\"ABCD\"      \"Susan\"             \"323\"     \"XYZ008\"");
         _resultingLines[4].Should().Be("\"ABCD\"      \"Mary\"              \"423\"     \"XYZ008\"");
         _resultingLines[5].Should().Be("\"XYZ\"       \"Mary\"              \"523\"     \"\"");
         _resultingLines[7].Should().Be("\"ABCD\"      \" Joan  \"           \"723\"     \"XYZ008\"");   //Joan has leading space (and 2 trailing spaces - not visible unless surrounded by quotes)
         _resultingLines[9].Should().Be("\"ABCD\"      \"Cindy\"             \"923\"     \"XYZ008\"");
         _resultingLines[10].Should().Be("\"\"          \"\"                  \"\"        \"\"");        //empty contents becomes quote pair for each field
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_QuotationModeStringsAndDates_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.QuotationMode = QuotationMode.StringsAndDates;
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "RECTYPE|12,NAME|20,NUM,ABCD_ID|8";
         _config.HeadersInFirstOutputRow = true;
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(11);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("\"RECTYPE\"   \"NAME\"              \"NUM\"     \"ABCD_I\"");  //note that ending quote is always provided, which may cut off traling characters from contents
         _resultingLines[1].Should().Be("\"XYZ\"       \"Mary, Ann\"         123");                     //note the empty last field is of type void and hence is not surrounded with quotes on output
         _resultingLines[3].Should().Be("\"ABCD\"      \"Susan\"             323       \"XYZ008\"");
         _resultingLines[4].Should().Be("\"ABCD\"      \"Mary\"              423       \"XYZ008\"");
         _resultingLines[5].Should().Be("\"XYZ\"       \"Mary\"              523");
         _resultingLines[7].Should().Be("\"ABCD\"      \" Joan  \"           723       \"XYZ008\"");   //Joan has leading space (and 2 trailing spaces - not visible unless surrounded by quotes)
         _resultingLines[9].Should().Be("\"ABCD\"      \"Cindy\"             923       \"XYZ008\"");
         _resultingLines[10].Should().Be(string.Empty);                                                //empty fields are void and therefore not surrounded with quotes
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_TrimOutputValues_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);  // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "NAME|12,DUMMY1|4,NUM";
         _config.HeadersInFirstOutputRow = true;
         _config.TrimOutputValues = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(11);

         _resultingLines.Count.Should().Be(12);

         _resultingLines[0].Should().Be("NAME        DUMMNUM       ");
         _resultingLines[1].Should().Be("Mary, Ann       123       ");
         _resultingLines[3].Should().Be("Susan           323       ");
         _resultingLines[7].Should().Be("Joan            723       ");
         _resultingLines[9].Should().Be("Cindy           923       ");
         _resultingLines[10].Should().Be("                          ");
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_ExcludeExtraneous_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.OutputFields = "NAME|12, DUMMY1|4, NUM";
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Count.Should().Be(11);

         _resultingLines[0].Should().Be("Mary, Ann       123");
         _resultingLines[2].Should().Be("Susan           323");
         _resultingLines[6].Should().Be(" Joan           723");
         _resultingLines[8].Should().Be("Cindy           923");
         _resultingLines[9].Should().Be(string.Empty);
         _resultingLines[10].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_NoOutputFieldsButDefaultWidth_UseDefaults()
      {
         //a_config.rrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.AllowOnTheFlyInputFields = true;  //otherwise every line would be left blank (note absence of InputFields setting)
                                                   //defaults... pass through
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.HeadersInFirstOutputRow = true;
         //No OutputFields
         _config.DefaultOutputFieldWidth = 15;
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

        //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(11);  //incl. header row

         _resultingLines.Count.Should().Be(12);  //1 header row + 10 data (incl. last with EOF, which is empty) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE        NAME           NUM            ABCD_ID        EOF");
         _resultingLines[1].Should().Be("XYZ            Mary, Ann      123");
         _resultingLines[2].Should().Be("ABCD           Mary           223            XYZ00883");
         _resultingLines[3].Should().Be("ABCD           Susan          323            XYZ00883");
         _resultingLines[4].Should().Be("ABCD           Mary           423            XYZ00883");
         _resultingLines[5].Should().Be("XYZ            Mary           523");
         _resultingLines[6].Should().Be("ABCD           Mary           623            XYZ00883");
         _resultingLines[7].Should().Be("ABCD            Joan          723            XYZ00883");
         _resultingLines[8].Should().Be("ABCD           Jane           823            XYZ00883");
         _resultingLines[9].Should().Be("ABCD           Cindy          923            XYZ00883");
         _resultingLines[10].Should().Be(string.Empty);  //EOF
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_NoOutputFieldsOrDefaultWidth_UseDefaultsWithWidthOf10()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.AllowOnTheFlyInputFields = true;  //otherwise every line would be left blank (note absence of InputFields setting)
                                                   //defaults... pass through
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.HeadersInFirstOutputRow = true;
         //No OutputFields
         //No ExcludeExtraneousFields - trailing spaces will be preserved

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(11);  //incl. header row

         _resultingLines.Count.Should().Be(12);  //1 header row + 10 data (incl. last with EOF, which is empty) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE   NAME      NUM       ABCD_ID   EOF       ");
         _resultingLines[1].Should().Be("XYZ       Mary, Ann 123                           ");
         _resultingLines[2].Should().Be("ABCD      Mary      223       XYZ00883            ");
         _resultingLines[3].Should().Be("ABCD      Susan     323       XYZ00883            ");
         _resultingLines[4].Should().Be("ABCD      Mary      423       XYZ00883            ");
         _resultingLines[5].Should().Be("XYZ       Mary      523                           ");
         _resultingLines[6].Should().Be("ABCD      Mary      623       XYZ00883            ");
         _resultingLines[7].Should().Be("ABCD       Joan     723       XYZ00883            ");
         _resultingLines[8].Should().Be("ABCD      Jane      823       XYZ00883            ");
         _resultingLines[9].Should().Be("ABCD      Cindy     923       XYZ00883            ");
         _resultingLines[10].Should().Be("                                                  ");  //EOF
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_MixedOutputFields_CorrectData()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.IntakeSupplier = _inLine;
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.AllowOnTheFlyInputFields = true;  //otherwise every line would be left blank (note absence of InputFields setting)
                                                   //defaults... pass through
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);  // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Flat;
         _config.HeadersInFirstOutputRow = true;
         _config.OutputFields = "RECTYPE,NAME|7,ABCD_dummy|4,ABCD_ID|4";  //RECTYPE will have a default width of 10
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(11);  //incl. header row

         _resultingLines.Count.Should().Be(12);  //1 header row + 10 data (incl. last with EOF, which is empty) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE   NAME   ABCDABCD");  //both ABCD_dummy and ABCD_ID show as ABCD on header
         _resultingLines[1].Should().Be("XYZ       Mary, A");
         _resultingLines[2].Should().Be("ABCD      Mary       XYZ0");
         _resultingLines[3].Should().Be("ABCD      Susan      XYZ0");
         _resultingLines[4].Should().Be("ABCD      Mary       XYZ0");
         _resultingLines[5].Should().Be("XYZ       Mary");
         _resultingLines[6].Should().Be("ABCD      Mary       XYZ0");
         _resultingLines[7].Should().Be("ABCD       Joan      XYZ0");
         _resultingLines[8].Should().Be("ABCD      Jane       XYZ0");
         _resultingLines[9].Should().Be("ABCD      Cindy      XYZ0");
         _resultingLines[10].Should().Be(string.Empty);  //EOF
         _resultingLines[11].Should().BeNull();
      }

   }
}