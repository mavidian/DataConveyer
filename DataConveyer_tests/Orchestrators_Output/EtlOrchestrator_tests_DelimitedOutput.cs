//EtlOrchestrator_tests_DelimitedOutput.cs
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
   public class EtlOrchestrator_tests_DelimitedOutput
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
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary\tLynn,@pNUM=423";
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
         _config.OutputDataKind = KindOfTextData.Delimited;
 
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Count.Should().Be(11);  //10 + null line at end

         _resultingLines[0].Should().Be("XYZ,\"Mary, Ann\",123,,");   //values containing commas are quoted even in absence of SurroundWitQuotes
         _resultingLines[2].Should().Be("ABCD,Susan,323,XYZ00883,");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[3].Should().Be("ABCD,Mary\tLynn,423,XYZ00883,");
         _resultingLines[6].Should().Be("ABCD, Joan  ,723,XYZ00883,");
         _resultingLines[8].Should().Be("ABCD,Cindy,923,XYZ00883,");  //sequence of fields needs to match preceding rows
         _resultingLines[9].Should().Be(",,,,");
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
         _config.OutputDataKind = KindOfTextData.Delimited;
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

         _resultingLines[0].Should().Be("RECTYPE,NAME,NUM,ABCD_ID,EOF");   //1st row contains field headers
         _resultingLines[1].Should().Be("XYZ,\"Mary, Ann\",123");   //values containing commas are quoted even in absence of SurroundWitQuotes
         _resultingLines[3].Should().Be("ABCD,Susan,323,XYZ00883");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[7].Should().Be("ABCD, Joan  ,723,XYZ00883");
         _resultingLines[9].Should().Be("ABCD,Cindy,923,XYZ00883");  //sequence of fields needs to match preceding rows
         _resultingLines[10].Should().Be(string.Empty);   //EOF has any empty value, so nothing left on line
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
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, "0000") : new ItemDef(ItemType.String, null);  //NUM Int - 4 digits, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputKeyPrefix = "**";
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Delimited;
         _config.HeadersInFirstOutputRow = true;
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config); ;

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(11);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE,NAME,NUM,ABCD_ID,EOF");   //OutputKeyPrefix is ignored in case of Delimited data
         _resultingLines[1].Should().Be("XYZ,\"Mary, Ann\",0123");   //values containing commas are quoted even in absence of SurroundWitQuotes
         _resultingLines[3].Should().Be("ABCD,Susan,0323,XYZ00883");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[7].Should().Be("ABCD, Joan  ,0723,XYZ00883");
         _resultingLines[9].Should().Be("ABCD,Cindy,0923,XYZ00883");  //sequence of fields needs to match preceding rows
         _resultingLines[10].Should().Be(string.Empty);   //EOF has any empty value, so nothing left on line
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
         _config.OutputDataKind = KindOfTextData.Delimited;
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

         _resultingLines.Count.Should().Be(12);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("\"RECTYPE\",\"NAME\",\"NUM\",\"ABCD_ID\",\"EOF\"");
         _resultingLines[1].Should().Be("\"XYZ\",\"Mary, Ann\",\"123\"");
         _resultingLines[3].Should().Be("\"ABCD\",\"Susan\",\"323\",\"XYZ00883\"");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[7].Should().Be("\"ABCD\",\" Joan  \",\"723\",\"XYZ00883\"");
         _resultingLines[9].Should().Be("\"ABCD\",\"Cindy\",\"923\",\"XYZ00883\"");  //sequence of fields needs to match preceding rows
         _resultingLines[10].Should().Be(string.Empty);   //EOF has any empty value, so nothing left on line
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
         _config.OutputDataKind = KindOfTextData.Delimited;
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

         _resultingLines.Count.Should().Be(12);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("\"RECTYPE\",\"NAME\",\"NUM\",\"ABCD_ID\",\"EOF\"");  //note that "NUM" (a label for int field) is string (not int) and hence surrounded by quotes
         _resultingLines[1].Should().Be("\"XYZ\",\"Mary, Ann\",123");
         _resultingLines[3].Should().Be("\"ABCD\",\"Susan\",323,\"XYZ00883\"");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[7].Should().Be("\"ABCD\",\" Joan  \",723,\"XYZ00883\"");
         _resultingLines[9].Should().Be("\"ABCD\",\"Cindy\",923,\"XYZ00883\"");  //sequence of fields needs to match preceding rows
         _resultingLines[10].Should().Be(string.Empty);   //EOF has any empty value, so nothing left on line
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
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Delimited;
         _config.OutputFields = "NAME, DUMMY1, NUM";
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

         _resultingLines[0].Should().Be("NAME,DUMMY1,NUM");
         _resultingLines[1].Should().Be("\"Mary, Ann\",,123");
         _resultingLines[3].Should().Be("Susan,,323");
         _resultingLines[7].Should().Be("Joan,,723");
         _resultingLines[9].Should().Be("Cindy,,923");
         _resultingLines[10].Should().Be(",,");
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_OutputFieldsWExclude_CorrectData()
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
         _config.OutputDataKind = KindOfTextData.Delimited;
         _config.OutputFields = "NAME, DUMMY1, NUM";
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

         _resultingLines.Count.Should().Be(12);

         _resultingLines[0].Should().Be("NAME,DUMMY1,NUM");
         _resultingLines[1].Should().Be("\"Mary, Ann\",,123");
         _resultingLines[3].Should().Be("Susan,,323");
         _resultingLines[7].Should().Be(" Joan  ,,723");
         _resultingLines[9].Should().Be("Cindy,,923");
         _resultingLines[10].Should().Be(string.Empty);
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_TabDelimitedQuoteAsNeeded_CorrectData()
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
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Delimited;
         _config.OutputFieldSeparator = '\t';
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

         _resultingLines.Count.Should().Be(12);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE\tNAME\tNUM\tABCD_ID\tEOF");
         _resultingLines[1].Should().Be("XYZ\tMary, Ann\t123");
         _resultingLines[3].Should().Be("ABCD\tSusan\t323\tXYZ00883");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[4].Should().Be("ABCD\t\"Mary\tLynn\"\t423\tXYZ00883");
         _resultingLines[7].Should().Be("ABCD\t Joan  \t723\tXYZ00883");
         _resultingLines[9].Should().Be("ABCD\tCindy\t923\tXYZ00883");  //sequence of fields needs to match preceding rows
         _resultingLines[10].Should().Be(string.Empty);   //EOF has any empty value, so nothing left on line
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_TabDelimitedQuoteAlways_CorrectData()
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
         _config.OutputDataKind = KindOfTextData.Delimited;
         _config.OutputFieldSeparator = '\t';
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

         _resultingLines.Count.Should().Be(12);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("\"RECTYPE\"\t\"NAME\"\t\"NUM\"\t\"ABCD_ID\"\t\"EOF\"");
         _resultingLines[1].Should().Be("\"XYZ\"\t\"Mary, Ann\"\t\"123\"");
         _resultingLines[3].Should().Be("\"ABCD\"\t\"Susan\"\t\"323\"\t\"XYZ00883\"");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[4].Should().Be("\"ABCD\"\t\"Mary\tLynn\"\t\"423\"\t\"XYZ00883\"");
         _resultingLines[7].Should().Be("\"ABCD\"\t\" Joan  \"\t\"723\"\t\"XYZ00883\"");
         _resultingLines[9].Should().Be("\"ABCD\"\t\"Cindy\"\t\"923\"\t\"XYZ00883\"");  //sequence of fields needs to match preceding rows
         _resultingLines[10].Should().Be(string.Empty);   //EOF has any empty value, so nothing left on line
         _resultingLines[11].Should().BeNull();
      }


      [TestMethod]
      public void processPipeline_DelimitedByTilde_CorrectData()
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
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted
         _config.OutputDataKind = KindOfTextData.Delimited;
         _config.OutputFieldSeparator = '~';
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

         _resultingLines.Count.Should().Be(12);

         _resultingLines.Count.Should().Be(12);  //1 header + 10 data (incl. last from EOF) + null (stream terminator)

         _resultingLines[0].Should().Be("RECTYPE~NAME~NUM~ABCD_ID~EOF");
         _resultingLines[1].Should().Be("XYZ~Mary, Ann~123");
         _resultingLines[3].Should().Be("ABCD~Susan~323~XYZ00883");  //sequence of fields needs to match preceding rows (that's why XYZ 000883 is last)
         _resultingLines[4].Should().Be("ABCD~Mary\tLynn~423~XYZ00883");
         _resultingLines[7].Should().Be("ABCD~ Joan  ~723~XYZ00883");
         _resultingLines[9].Should().Be("ABCD~Cindy~923~XYZ00883");  //sequence of fields needs to match preceding rows
         _resultingLines[10].Should().Be(string.Empty);   //EOF has any empty value, so nothing left on line
         _resultingLines[11].Should().BeNull();
      }
   }
}