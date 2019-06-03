//EtlOrchestrator_tests_ArbitraryOutput.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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


using DataConveyer.Tests.TestHelpers;
using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Generic;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Output
{
   public class EtlOrchestrator_tests_ArbitraryOutput
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=\"Susan   \",@pNUM=323";  //note 3 spaces after Susan
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "EOF";
      }

      //Results of the tests are held here:
      private readonly List<string> _resultingLines;

      public EtlOrchestrator_tests_ArbitraryOutput()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Keyword
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.Arbitrary;
         _config.ArbitraryOutputDefs = new string[] {
                                                       "Record type is {RECTYPE},",
                                                       " name is {NAME}",
                                                       " and number is {NUM}.",
                                                       " Void item here."
                                                    };
         _config.SetOutputConsumer(l => { if (l != null) _resultingLines.Add(l); });

         _resultingLines = new List<string>();
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputSimpleConfig_CorrectData()
      {
         //arrange
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("Record type is XYZ, name is Mary and number is 123. Void item here.");
         _resultingLines[2].Should().Be("Record type is ABCD, name is Susan    and number is 323. Void item here.");
         _resultingLines[4].Should().Be("Record type is , name is  and number is . Void item here.");   //EOF, all 3 tokens are empty
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputTrickyConfig_CorrectData()
      {
         //arrange
         _config.ArbitraryOutputDefs = new string[] {
                                                       "These fields have multiple tokens each, but only the first one gets substituted:",
                                                       " rectype, name, num and eof: {RECTYPE}, {NAME}, {NUM} and {EOF};",
                                                       " num and name stay unchanged when dummy is first{DUMMY}: {NUM} and {NAME};",
                                                       " eof is absent from all rows, but last: {EOF} and {NAME}."  //but even in last row EOF value is empty
                                                    };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("These fields have multiple tokens each, but only the first one gets substituted: rectype, name, num and eof: XYZ, {NAME}, {NUM} and {EOF}; num and name stay unchanged when dummy is first: {NUM} and {NAME}; eof is absent from all rows, but last:  and {NAME}.");
         _resultingLines[2].Should().Be("These fields have multiple tokens each, but only the first one gets substituted: rectype, name, num and eof: ABCD, {NAME}, {NUM} and {EOF}; num and name stay unchanged when dummy is first: {NUM} and {NAME}; eof is absent from all rows, but last:  and {NAME}.");
         _resultingLines[4].Should().Be("These fields have multiple tokens each, but only the first one gets substituted: rectype, name, num and eof: , {NAME}, {NUM} and {EOF}; num and name stay unchanged when dummy is first: {NUM} and {NAME}; eof is absent from all rows, but last:  and {NAME}.");
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputParametersInConfig_CorrectData()
      {
         //arrange
         _config.HeadersInFirstInputRow = true;  //ignored
         _config.TrimOutputValues = true;
         _config.OutputKeyPrefix = "###";  //ignored

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("Record type is XYZ, name is Mary and number is 123. Void item here.");
         _resultingLines[2].Should().Be("Record type is ABCD, name is Susan and number is 323. Void item here.");  //note removed spaces after Susan
         _resultingLines[4].Should().Be("Record type is , name is  and number is . Void item here.");   //EOF, all 3 tokens are empty
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputSurroundWQuotes_CorrectData()
      {
         //arrange

         _config.TrimOutputValues = true;
         _config.QuotationMode = QuotationMode.Always;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("Record type is \"XYZ\", name is \"Mary\" and number is \"123\". Void item here.");
         _resultingLines[2].Should().Be("Record type is \"ABCD\", name is \"Susan\" and number is \"323\". Void item here.");  //note removed spaces after Susan
         _resultingLines[4].Should().Be("Record type is \"\", name is \"\" and number is \"\". Void item here.");   //EOF, all 3 tokens are empty
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputEscapedBraces_CorrectData()
      {
         //arrange
         _config.ArbitraryOutputDefs = new string[] {
                                                  "\\{Record type is {RECTYPE},",  //each \\ pair represents a single \
                                                  " name \\{is\\} {NAME}.",
                                                  " Void \\{item} here."
                                               };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("{Record type is XYZ, name {is} Mary. Void {item} here.");
         _resultingLines[2].Should().Be("{Record type is ABCD, name {is} Susan   . Void {item} here.");
         _resultingLines[4].Should().Be("{Record type is , name {is} . Void {item} here.");
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputEscapedBracesAndBackslashes_CorrectData()
      {
         //arrange
         _config.ArbitraryOutputDefs = new string[] {
                                                  "\\\\{Record type is {RECTYPE},",  //each \\ pair represents a single \
                                                  " name \\\\{is\\\\} {NAME}.",
                                                  " Void \\\\{item} here."
                                               };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("\\{Record type is XYZ, name \\{is\\} Mary. Void \\{item} here.");
         _resultingLines[2].Should().Be("\\{Record type is ABCD, name \\{is\\} Susan   . Void \\{item} here.");
         _resultingLines[4].Should().Be("\\{Record type is , name \\{is\\} . Void \\{item} here.");
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputNonExistingKey_BlankOutput()
      {
         //arrange
         _config.ArbitraryOutputDefs = new string[] {
                                                  "Record type is {RECTYPE},",
                                                  " nothing here: <<{BADKEY}>>.",
                                                  " Void item here."
                                               };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("Record type is XYZ, nothing here: <<>>. Void item here.");
         _resultingLines[2].Should().Be("Record type is ABCD, nothing here: <<>>. Void item here.");
         _resultingLines[4].Should().Be("Record type is , nothing here: <<>>. Void item here.");   //EOF, tokens are empty
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputNullDefs_NullDefsIgnored()
      {
         //arrange
         _config.ArbitraryOutputDefs = new string[] {
                                                  "Record type is {RECTYPE},",
                                                  null,
                                                  " nothing here: <<{BADKEY}>>.",
                                                  " Void item here.",
                                                  null
                                               };

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("Record type is XYZ, nothing here: <<>>. Void item here.");
         _resultingLines[2].Should().Be("Record type is ABCD, nothing here: <<>>. Void item here.");
         _resultingLines[4].Should().Be("Record type is , nothing here: <<>>. Void item here.");   //EOF, tokens are empty
      }


      [Fact]
      public void ProcessPipeline_ArbitraryOutputNoDefs_EmptyOutput()
      {
         //arrange
         _config.ArbitraryOutputDefs = null;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("");
         _resultingLines[2].Should().Be("");
         _resultingLines[4].Should().Be("");   //EOF
      }

   }
}
