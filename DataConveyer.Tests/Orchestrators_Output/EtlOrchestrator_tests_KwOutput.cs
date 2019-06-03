//EtlOrchestrator_tests_KwOutput.cs
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
   public class EtlOrchestrator_tests_KwOutput
   {
      private readonly OrchestratorConfig _config;

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
         yield return "@pABCD_ID=XYZ00883,@pNAME=Cindy,@pNUM=923,@pRECTYPE=ABCD";
         yield return "EOF";
      }

      //Results of the tests are held here:
      private readonly List<string> _resultingLines;

      public EtlOrchestrator_tests_KwOutput()
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
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.Keyword;
         _config.SetOutputConsumer(l => _resultingLines.Add(l));

         _resultingLines = new List<string>();
      }


      [Fact]
      public void ProcessPipeline_SimpleSettings_CorrectData()
      {
         //arrange
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Should().HaveCount(11); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("RECTYPE=XYZ,NAME=Mary,NUM=123");  //same as input, except for stripped InputKeyPrefix and insignificant whitespace
         _resultingLines[2].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Susan,NUM=323");
         _resultingLines[3].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=\"Mary,Ann\",NUM=423");
         _resultingLines[6].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Joan,NUM=723");
         _resultingLines[8].Should().Be("ABCD_ID=XYZ00883,NAME=Cindy,NUM=923,RECTYPE=ABCD");
         _resultingLines[9].Should().Be("EOF");
         _resultingLines[10].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_TrailerContents_CorrectData()
      {
         //arrange
         _config.TrailerContents = "SampleTrailer";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(11);  //10 input rows + 1 trailer line

         _resultingLines.Should().HaveCount(12); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("RECTYPE=XYZ,NAME=Mary,NUM=123");  //same as input, except for stripped InputKeyPrefix and insignificant whitespace
         _resultingLines[2].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Susan,NUM=323");
         _resultingLines[3].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=\"Mary,Ann\",NUM=423");
         _resultingLines[6].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Joan,NUM=723");
         _resultingLines[8].Should().Be("ABCD_ID=XYZ00883,NAME=Cindy,NUM=923,RECTYPE=ABCD");
         _resultingLines[9].Should().Be("EOF");
         _resultingLines[10].Should().Be("SampleTrailer");
         _resultingLines[11].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_MultilineTrailerContents_CorrectData()
      {
         //arrange

         _config.TrailerContents = "TrailerOne\r\nTrailer Two";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(12);  //10 input rows + 2 trailer lines

         _resultingLines.Should().HaveCount(13); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("RECTYPE=XYZ,NAME=Mary,NUM=123");  //same as input, except for stripped InputKeyPrefix and insignificant whitespace
         _resultingLines[2].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Susan,NUM=323");
         _resultingLines[3].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=\"Mary,Ann\",NUM=423");
         _resultingLines[6].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Joan,NUM=723");
         _resultingLines[8].Should().Be("ABCD_ID=XYZ00883,NAME=Cindy,NUM=923,RECTYPE=ABCD");
         _resultingLines[9].Should().Be("EOF");
         _resultingLines[10].Should().Be("TrailerOne");
         _resultingLines[11].Should().Be("Trailer Two");
         _resultingLines[12].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_LeaderContents_CorrectData()
      {
         //arrange
         _config.LeaderContents = "SampleLeader";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(11);  //10 input rows + 1 leader line

         _resultingLines.Should().HaveCount(12); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("SampleLeader");
         _resultingLines[1].Should().Be("RECTYPE=XYZ,NAME=Mary,NUM=123");  //same as input, except for stripped InputKeyPrefix and insignificant whitespace
         _resultingLines[3].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Susan,NUM=323");
         _resultingLines[4].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=\"Mary,Ann\",NUM=423");
         _resultingLines[7].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Joan,NUM=723");
         _resultingLines[9].Should().Be("ABCD_ID=XYZ00883,NAME=Cindy,NUM=923,RECTYPE=ABCD");
         _resultingLines[10].Should().Be("EOF");
         _resultingLines[11].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_MultilineLeaderContents_CorrectData()
      {
         //arrange
         _config.LeaderContents = "Leader One\r\nLeaderTwo";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(12);  //10 input rows + 2 trailer lines

         _resultingLines.Should().HaveCount(13); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("Leader One");
         _resultingLines[1].Should().Be("LeaderTwo");
         _resultingLines[2].Should().Be("RECTYPE=XYZ,NAME=Mary,NUM=123");  //same as input, except for stripped InputKeyPrefix and insignificant whitespace
         _resultingLines[4].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Susan,NUM=323");
         _resultingLines[5].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=\"Mary,Ann\",NUM=423");
         _resultingLines[8].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Joan,NUM=723");
         _resultingLines[10].Should().Be("ABCD_ID=XYZ00883,NAME=Cindy,NUM=923,RECTYPE=ABCD");
         _resultingLines[11].Should().Be("EOF");
         _resultingLines[12].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_OutputKeyPrefixAlsoFormat_CorrectData()
      {
         //arrange
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, "0000") : new ItemDef(ItemType.String, null);  //NUM Int - 4 digits, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.OutputKeyPrefix = "#!#";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Should().HaveCount(11); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("#!#RECTYPE=XYZ,#!#NAME=Mary,#!#NUM=0123");
         _resultingLines[2].Should().Be("#!#RECTYPE=ABCD,#!#ABCD_ID=XYZ00883,#!#NAME=Susan,#!#NUM=0323");
         _resultingLines[3].Should().Be("#!#RECTYPE=ABCD,#!#ABCD_ID=XYZ00883,#!#NAME=\"Mary,Ann\",#!#NUM=0423");
         _resultingLines[7].Should().Be("#!#RECTYPE=ABCD,#!#ABCD_ID=XYZ00883,#!#NAME=Jane,#!#NUM=0823");
         _resultingLines[8].Should().Be("#!#ABCD_ID=XYZ00883,#!#NAME=Cindy,#!#NUM=0923,#!#RECTYPE=ABCD");
         _resultingLines[9].Should().Be("#!#EOF");
         _resultingLines[10].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_QuotationModeAlways_CorrectData()
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.QuotationMode = QuotationMode.Always;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Should().HaveCount(11); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("RECTYPE=\"XYZ\",NAME=\"Mary\",NUM=\"123\"");
         _resultingLines[2].Should().Be("RECTYPE=\"ABCD\",ABCD_ID=\"XYZ00883\",NAME=\"Susan\",NUM=\"323\"");
         _resultingLines[3].Should().Be("RECTYPE=\"ABCD\",ABCD_ID=\"XYZ00883\",NAME=\"Mary,Ann\",NUM=\"423\"");
         _resultingLines[6].Should().Be("RECTYPE=\"ABCD\",ABCD_ID=\"XYZ00883\",NAME=\"Joan\",NUM=\"723\"");
         _resultingLines[8].Should().Be("ABCD_ID=\"XYZ00883\",NAME=\"Cindy\",NUM=\"923\",RECTYPE=\"ABCD\"");
         _resultingLines[9].Should().Be("EOF");
         _resultingLines[10].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_QuotationModeStringsAndDates_CorrectData()
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.QuotationMode = QuotationMode.StringsAndDates;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Should().HaveCount(11); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("RECTYPE=\"XYZ\",NAME=\"Mary\",NUM=123");
         _resultingLines[2].Should().Be("RECTYPE=\"ABCD\",ABCD_ID=\"XYZ00883\",NAME=\"Susan\",NUM=323");
         _resultingLines[3].Should().Be("RECTYPE=\"ABCD\",ABCD_ID=\"XYZ00883\",NAME=\"Mary,Ann\",NUM=423");
         _resultingLines[6].Should().Be("RECTYPE=\"ABCD\",ABCD_ID=\"XYZ00883\",NAME=\"Joan\",NUM=723");
         _resultingLines[8].Should().Be("ABCD_ID=\"XYZ00883\",NAME=\"Cindy\",NUM=923,RECTYPE=\"ABCD\"");
         _resultingLines[9].Should().Be("EOF");
         _resultingLines[10].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_QuotationModeOnlyIfNeeded_CorrectData()
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => true;  //each record is it's own cluster
         _config.QuotationMode = QuotationMode.OnlyIfNeeded;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(10);
         counts.ClustersWritten.Should().Be(10);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Should().HaveCount(11); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("RECTYPE=XYZ,NAME=Mary,NUM=123");
         _resultingLines[2].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Susan,NUM=323");
         _resultingLines[3].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=\"Mary,Ann\",NUM=423");
         _resultingLines[6].Should().Be("RECTYPE=ABCD,ABCD_ID=XYZ00883,NAME=Joan,NUM=723");
         _resultingLines[8].Should().Be("ABCD_ID=XYZ00883,NAME=Cindy,NUM=923,RECTYPE=ABCD");
         _resultingLines[9].Should().Be("EOF");
         _resultingLines[10].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_OutputFields_CorrectData()
      {
         //arrange
         _config.OutputFields = "NAME, DUMMY1, RECTYPE, DUMMY2, NUM";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Should().HaveCount(11); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("NAME=Mary,DUMMY1,RECTYPE=XYZ,DUMMY2,NUM=123");
         _resultingLines[2].Should().Be("NAME=Susan,DUMMY1,RECTYPE=ABCD,DUMMY2,NUM=323");
         _resultingLines[3].Should().Be("NAME=\"Mary,Ann\",DUMMY1,RECTYPE=ABCD,DUMMY2,NUM=423");
         _resultingLines[6].Should().Be("NAME=Joan,DUMMY1,RECTYPE=ABCD,DUMMY2,NUM=723");
         _resultingLines[8].Should().Be("NAME=Cindy,DUMMY1,RECTYPE=ABCD,DUMMY2,NUM=923");
         _resultingLines[9].Should().Be("NAME,DUMMY1,RECTYPE,DUMMY2,NUM");
         _resultingLines[10].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_OutputFieldsWithExclude_CorrectData()
      {
         //arrange
         _config.OutputFields = "NAME, DUMMY1, RECTYPE, DUMMY2, NUM";
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(10);
         counts.ClustersRead.Should().Be(2);
         counts.ClustersWritten.Should().Be(2);
         counts.RowsWritten.Should().Be(10);

         _resultingLines.Should().HaveCount(11); //incl. EOD mark (null)

         _resultingLines[0].Should().Be("NAME=Mary,RECTYPE=XYZ,NUM=123");
         _resultingLines[2].Should().Be("NAME=Susan,RECTYPE=ABCD,NUM=323");
         _resultingLines[3].Should().Be("NAME=\"Mary,Ann\",RECTYPE=ABCD,NUM=423");
         _resultingLines[6].Should().Be("NAME=Joan,RECTYPE=ABCD,NUM=723");
         _resultingLines[8].Should().Be("NAME=Cindy,RECTYPE=ABCD,NUM=923");
         _resultingLines[9].Should().Be(string.Empty);  //here, none of the output fields were in the records, so all were excluded
         _resultingLines[10].Should().BeNull();
      }


   }
}
