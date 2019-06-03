//EtlOrchestrator_tests_KwIntake.cs
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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_KwIntake
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=\"XYZ\",  @pABCD_ID=  \"XYZ00883\",@NUM=123,@pNAME=\"Mary, Ann\"";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=XYZ00883,@NUM=123,@pNAME=\"Mary, Ann\"";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=123";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@NUM=123,@pNAME=\"Mary, Ann\"";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=123";
         yield return "@pRECTYPE=\"XYZ\",@pABCD_ID= XYZ00883,@NUM=123,@pNAME=\"Mary, Ann\"";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\" XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=123";
         yield return "@pRECTYPE=\"ABCD\",@NUM=123,@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\"";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=123";
         yield return "@pRECTYPE=\"ABCD\",@pABCD_ID=\"XYZ00883\",@pNAME=\"Mary, Ann\",@NUM=123";
         yield return "EOF=";  //default value (for string: string.Empty)
         yield return "EOF";   //null value (for value types same as default value)
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_KwIntake()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Keyword
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.RetainQuotes = false;
         _config.AllowOnTheFlyInputFields = true;
         _config.DeferTransformation = DeferTransformation.Indefinitely;  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessPipeline_SimpleConfig_CorrectData()
      {
         //arrange
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);
         resultingClusters[0].Count.Should().Be(5);
         resultingClusters[1].Count.Should().Be(7);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["BadKey"].Should().BeNull();
         kvRec["ABCD_ID"].Should().Be("XYZ00883");  //ignored whitespace in front of quote
         kvRec["NAME"].Should().Be("Mary, Ann");
         kvRec["@NUM"].Should().Be("123");  //note the prefix mismatch

         kvRec = resultingClusters[0][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["ABCD_ID"].Should().Be("XYZ00883");
         kvRec["NAME"].Should().Be("Mary, Ann");

         kvRec = resultingClusters[1][0];
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["ABCD_ID"].Should().Be(" XYZ00883");  //unquoted with leading whitespace
         kvRec["@NUM"].Should().Be("123");

         kvRec = resultingClusters[1][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["ABCD_ID"].Should().Be(" XYZ00883");  //leading whitespace within quotes
         kvRec["NAME"].Should().Be("Mary, Ann");

         kvRec = resultingClusters[1][5];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().Be(string.Empty);  //note the prefix mismatch

         kvRec = resultingClusters[1][6];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().BeNull();  //note the prefix mismatch
      }


      [Fact]
      public void ProcessPipeline_InputFieldsDefined_CorrectData()
      {
         //arrange
         _config.InputKeyPrefix = "@";
         _config.InputFields = "pRECTYPE,NUM,pNAME";  //only these 3 fields can be extracted (i.e. item count can be 0, 1, 2 or 3)
         _config.AllowOnTheFlyInputFields = false;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(12);  //each record own cluster in this test
         resultingClusters[0].Count.Should().Be(1);
         resultingClusters[1].Count.Should().Be(1);
         resultingClusters[10].Count.Should().Be(1);
         resultingClusters[11].Count.Should().Be(1);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(3);
         kvRec["pRECTYPE"].Should().Be("XYZ");
         kvRec["BadKey"].Should().BeNull();
         kvRec["pNAME"].Should().Be("Mary, Ann");
         kvRec["NUM"].Should().Be("123");

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(3);
         kvRec["pRECTYPE"].Should().Be("ABCD");
         kvRec["pNAME"].Should().Be("Mary, Ann");

         kvRec = resultingClusters[5][0];
         kvRec["pRECTYPE"].Should().Be("XYZ");
         kvRec["NUM"].Should().Be("123");

         kvRec = resultingClusters[6][0];
         kvRec.Count.Should().Be(3);
         kvRec["pRECTYPE"].Should().Be("ABCD");
         kvRec["pNAME"].Should().Be("Mary, Ann");

         kvRec = resultingClusters[10][0];
         kvRec.Count.Should().Be(0);

         kvRec = resultingClusters[11][0];
         kvRec.Count.Should().Be(0);
      }


      [Fact]
      public void ProcessPipeline_SingleRecClusterExclMissPfx_CorrectData()
      {
         //arrange
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = true;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
      
         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(12);
         resultingClusters[0].Count.Should().Be(1);
         resultingClusters[1].Count.Should().Be(1);
         resultingClusters[11].Count.Should().Be(1);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["BadKey"].Should().BeNull();
         kvRec["NAME"].Should().Be("Mary, Ann");
         kvRec["@NUM"].Should().BeNull();
         kvRec["NUM"].Should().BeNull();

         Action a = () => { var x = resultingClusters[0][1]; };  //attempt to access non-existent record in cluster
         a.Should().Throw<ArgumentOutOfRangeException>();

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Mary, Ann");

         kvRec = resultingClusters[5][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["NAME"].Should().Be("Mary, Ann");

         kvRec = resultingClusters[6][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Mary, Ann");
         kvRec["@NUM"].Should().BeNull();
         kvRec["NUM"].Should().BeNull();

         kvRec = resultingClusters[10][0];  //record with no items (the only key was excluded due to prefix mismatch)
         kvRec.Count.Should().Be(0);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().BeNull();

         kvRec = resultingClusters[11][0];  //this is also a record with no items
         kvRec.Count.Should().Be(0);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_NoPrefixRetainQuotes_CorrectData()
      {
         //arrange

         _config.RetainQuotes = true;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(12);
         resultingClusters[0].Count.Should().Be(1);
         resultingClusters[1].Count.Should().Be(1);
         resultingClusters[11].Count.Should().Be(1);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["@pRECTYPE"].Should().Be("\"XYZ\"");
         kvRec["BadKey"].Should().BeNull();
         kvRec["@pNAME"].Should().Be("\"Mary, Ann\"");
         kvRec["@NUM"].Should().Be("123");
         kvRec["NUM"].Should().BeNull();

         Action a = () => { var x = resultingClusters[0][1]; };  //attempt to access non-existent record in clusted
         a.Should().Throw<ArgumentOutOfRangeException>();

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(4);
         kvRec["@pRECTYPE"].Should().Be("\"ABCD\"");
         kvRec["@pNAME"].Should().Be("\"Mary, Ann\"");

         kvRec = resultingClusters[5][0];
         kvRec.Count.Should().Be(4);
         kvRec["@pRECTYPE"].Should().Be("\"XYZ\"");
         kvRec["@pNAME"].Should().Be("\"Mary, Ann\"");

         kvRec = resultingClusters[6][0];
         kvRec.Count.Should().Be(4);
         kvRec["@pRECTYPE"].Should().Be("\"ABCD\"");
         kvRec["@pNAME"].Should().Be("\"Mary, Ann\"");
         kvRec["@NUM"].Should().Be("123");
         kvRec["NUM"].Should().BeNull();

         kvRec = resultingClusters[10][0];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().Be(string.Empty); // default for string, in case nothing after = (e.g. EOF=)

         kvRec = resultingClusters[11][0];
         kvRec.Count.Should().Be(1);
         kvRec["@pRECTYPE"].Should().BeNull();  //here, null means no item for the key
         kvRec.GetItem("@pRECTYPE").ItemDef.Type.Should().Be(ItemType.Void);
         kvRec["EOF"].Should().BeNull();        //here, null means item exists, but its value is null (e.g. EOF)
         kvRec.GetItem("EOF").ItemDef.Type.Should().NotBe(ItemType.Void);
      }


      [Fact]
      public void ProcessPipeline_EmptyDateTimeValue_DefaultAssumed()
      {
         //arrange
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key =>
         {
            return key == "EOF" ? new ItemDef(ItemType.DateTime, null)
                                : new ItemDef(ItemType.String, null);
         }; //make EOF field DateTime, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(12);
         resultingClusters[0].Count.Should().Be(1);
         resultingClusters[1].Count.Should().Be(1);
         resultingClusters[11].Count.Should().Be(1);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(4);
         kvRec["@pRECTYPE"].Should().Be("XYZ");

         kvRec = resultingClusters[10][0];  //EOF=
         kvRec.Count.Should().Be(1);
         kvRec.GetItem("EOF").ItemDef.Type.Should().Be(ItemType.DateTime);
         kvRec["EOF"].Should().Be(default(DateTime));

         kvRec = resultingClusters[11][0];  //EOF   for value types like DateTime, either empty field (EOF=) or null field (EOF) results in default value
         kvRec.Count.Should().Be(1);
         kvRec.GetItem("EOF").ItemDef.Type.Should().Be(ItemType.DateTime);
         kvRec["EOF"].Should().Be(default(DateTime));
      }


      [Fact]
      public void ProcessPipeline_AllFieldsInt_CorrectData()
      {
         //arrange
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => { return new ItemDef(ItemType.Int, null); }; //everything int
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         //everything, needs to be int, so in most cases, it is simply 0, i.e. default(int)
         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(4);
         kvRec["@pRECTYPE"].Should().Be(default(int));
         kvRec.GetItem("@pRECTYPE").ItemDef.Type.Should().Be(ItemType.Int);
         kvRec["@pABCD_ID"].Should().Be(default(int));
         kvRec["@pNAME"].Should().Be(default(int));
         kvRec["@NUM"].Should().Be(123);

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(4);
         kvRec["@pRECTYPE"].Should().Be(default(int));
         kvRec["@pABCD_ID"].Should().Be(default(int));
         kvRec["@pNAME"].Should().Be(default(int));
         kvRec["@NUM"].Should().Be(123);

         kvRec = resultingClusters[10][0];
         kvRec.Count.Should().Be(1);
         kvRec.GetItem("EOF").ItemDef.Type.Should().Be(ItemType.Int);
         kvRec["EOF"].Should().Be(default(int));

         kvRec = resultingClusters[11][0];
         kvRec.Count.Should().Be(1);
         kvRec.GetItem("EOF").ItemDef.Type.Should().Be(ItemType.Int);
         kvRec["EOF"].Should().Be(default(int));
      }


      [Fact]
      public void ProcessPipeline_ClusterSplitAfterPredicate_CorrectRecCounts()
      {
         //arrange
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["@pRECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ mark end of the cluster
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster 

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Count.Should().Be(3);
         resultingClusters[0].Count.Should().Be(1);
         resultingClusters[1].Count.Should().Be(5);
         resultingClusters[2].Count.Should().Be(6);
      }


      [Fact]
      public void ProcessPipeline_HeadersInFirstInputRow_SettingIgnored()
      {
         //arrange
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.HeadersInFirstInputRow = true;  //this setting should be ignored in case of KW data
         _config.AllowOnTheFlyInputFields = true;
         _config.OutputConsumer = (t, gc) => { };  //throwaway consumer

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(12);
         resultingClusters[0].Count.Should().Be(1);  //this cluster is created from the 1st row (to be treated as data row in spite of HeadersInFirstInputRow setting)
         resultingClusters[0][0].Count.Should().Be(4);
         resultingClusters[0][0][0].Should().Be("XYZ");
         resultingClusters[0][0][2].Should().Be("123");
      }


      [Fact]
      public void ProcessPipeline_SimpleConfigPipeDelimited_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines().Select(l => l.Replace(',', '|'))).StringSupplier);      
         _config.InputFieldSeparator = '|';
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);
         resultingClusters[0].Count.Should().Be(5);
         resultingClusters[1].Count.Should().Be(7);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["BadKey"].Should().BeNull();
         kvRec["ABCD_ID"].Should().Be("XYZ00883");
         kvRec["NAME"].Should().Be("Mary| Ann");
         kvRec["@NUM"].Should().Be("123");  //note the prefix mismatch

         kvRec = resultingClusters[0][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["ABCD_ID"].Should().Be("XYZ00883");
         kvRec["NAME"].Should().Be("Mary| Ann");

         kvRec = resultingClusters[1][0];
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["ABCD_ID"].Should().Be(" XYZ00883");  //unquoted with leading whitespace
         kvRec["@NUM"].Should().Be("123");

         kvRec = resultingClusters[1][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["ABCD_ID"].Should().Be(" XYZ00883");
         kvRec["NAME"].Should().Be("Mary| Ann");

         kvRec = resultingClusters[1][5];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().Be(string.Empty);  //note the prefix mismatch

         kvRec = resultingClusters[1][6];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().BeNull();  //note the prefix mismatch
      }


      [Fact]
      public void ProcessPipeline_SimpleConfigTabDelimited_CorrectData()
      {
         //arrange
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines().Select(l => l.Replace(',', '\t'))).StringSupplier);
         _config.InputFieldSeparator = '\t';
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);
         resultingClusters[0].Count.Should().Be(5);
         resultingClusters[1].Count.Should().Be(7);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["BadKey"].Should().BeNull();
         kvRec["ABCD_ID"].Should().Be("XYZ00883");
         kvRec["NAME"].Should().Be("Mary\t Ann");
         kvRec["@NUM"].Should().Be("123");  //note the prefix mismatch

         kvRec = resultingClusters[0][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["ABCD_ID"].Should().Be("XYZ00883");
         kvRec["NAME"].Should().Be("Mary\t Ann");

         kvRec = resultingClusters[1][0];
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["ABCD_ID"].Should().Be(" XYZ00883");  //unquoted with leading whitespace
         kvRec["@NUM"].Should().Be("123");

         kvRec = resultingClusters[1][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["ABCD_ID"].Should().Be(" XYZ00883");
         kvRec["NAME"].Should().Be("Mary\t Ann");

         kvRec = resultingClusters[1][5];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().Be(string.Empty);  //note the prefix mismatch

         kvRec = resultingClusters[1][6];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();
         kvRec["EOF"].Should().BeNull();  //note the prefix mismatch
      }

   }
}
