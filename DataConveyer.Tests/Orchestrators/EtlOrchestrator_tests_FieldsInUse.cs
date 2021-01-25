//EtlOrchestrator_tests_FieldsInUse.cs
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


using DataConveyer.Tests.TestHelpers;
using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Intake;
using Mavidian.DataConveyer.Orchestrators;
using Mavidian.DataConveyer.Transform;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   /// <summary>
   /// Tests specific to FieldsInUse property of IntakeProvider and TransformProvider classes
   /// </summary>
   public class EtlOrchestrator_tests_FieldsInUse
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pFld1=\"XYZ\",@pFld2=\"XYZ00883\",@pFld3=\"Mary\",@pFld4=123";
         yield return "@pFld3,@pFld1=\"XYZ00883\",@pFld2=\"Mary\",@pFld5=123";
         yield return "@pFld6=\"ABCD\",@pFld5=\"XYZ00883\"";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_FieldsInUse()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Keyword
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster          _config.AllowOnTheFlyInputFields = true;
         _config.DeferTransformation = DeferTransformation.Indefinitely;  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void FieldsInUseOnIntake_InterweavedKeys_CorrectFieldsInUse()
      {
         //arrange
         _config.ExcludeItemsMissingPrefix = true;
         _config.AllowOnTheFlyInputFields = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         var flds = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;

         //assert
         flds.Should().HaveCount(6);
         flds[0].Should().Be("Fld1");
         flds[1].Should().Be("Fld2");
         flds[2].Should().Be("Fld3");
         flds[3].Should().Be("Fld4");
         flds[4].Should().Be("Fld5");
         flds[5].Should().Be("Fld6");
         // _resulting clusters not used in this test
      }


      [Fact]
      public void FieldsInUseOnIntake_DisallowOnTheFlyFields_CorrectData()
      {
         //arrange
         _config.ExcludeItemsMissingPrefix = true;
         _config.ExplicitTypeDefinitions = "Fld4|I";  // Fld4 int, everything else string
         _config.InputFields = "Fld5,Fld4,Fld1";
         _config.AllowOnTheFlyInputFields = false;
         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         var resultingClusters = _resultingClusters.ToList();
         var flds = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;

         //assert
         flds.Count.Should().Be(3);  //FieldsInUse - only 3 of the 5 fields are accepted from intake
         flds[0].Should().Be("Fld5");
         flds[1].Should().Be("Fld4");
         flds[2].Should().Be("Fld1");

         var kvRec = resultingClusters[0][0];  //1st data row
         kvRec.Count.Should().Be(2);
         kvRec.Keys[0].Should().Be("Fld1");
         kvRec["Fld1"].Should().Be("XYZ");
         kvRec.GetItem(1).Key.Should().Be("Fld4");
         kvRec["Fld4"].Should().Be(123);

         kvRec = resultingClusters[1][0];  //2nd data row
         kvRec.Count.Should().Be(2);
         kvRec.Keys[0].Should().Be("Fld1");
         kvRec["Fld1"].Should().Be("XYZ00883");
         kvRec.GetItem(1).Key.Should().Be("Fld5");
         kvRec["Fld5"].Should().Be("123");

         kvRec = resultingClusters[2][0];  //3rd data row
         kvRec.Count.Should().Be(1);
         kvRec.Keys[0].Should().Be("Fld5");
         kvRec["Fld5"].Should().Be("XYZ00883");
      }

      [Fact]
      public void FieldsInUseOnIntake_RecordWithNoItems_CorrectData()
      {
         //arrange
         _config.ExcludeItemsMissingPrefix = true;
         _config.ExplicitTypeDefinitions = "Fld4|I";  // Fld4 int, everything else string
         _config.InputFields = "Fld4,Fld1";
         _config.AllowOnTheFlyInputFields = false;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         var resultingClusters = _resultingClusters.ToList();
         var flds = ((IntakeProvider)(new PrivateAccessor(orchestrator)).GetField("_intakeProvider")).FieldsInUse;

         //assert
         flds.Count.Should().Be(2);  //FieldsInUse
         flds[0].Should().Be("Fld4");
         flds[1].Should().Be("Fld1");

         var kvRec = resultingClusters[0][0];  //1st data row
         kvRec.RecNo.Should().Be(1);
         kvRec.Count.Should().Be(2);
         kvRec.Keys[0].Should().Be("Fld1");
         kvRec["Fld1"].Should().Be("XYZ");
         kvRec.GetItem(1).Key.Should().Be("Fld4");
         kvRec["Fld4"].Should().Be(123);

         kvRec = resultingClusters[1][0];  //2nd data row
         kvRec.RecNo.Should().Be(2);
         kvRec.Count.Should().Be(1);
         kvRec.Keys[0].Should().Be("Fld1");
         kvRec["Fld1"].Should().Be("XYZ00883");

         kvRec = resultingClusters[2][0];  //3rd data row - this row is empty
         kvRec.RecNo.Should().Be(3);
         kvRec.Count.Should().Be(0);
      }


      [Fact]
      public void FieldsInUseOnTransform_DisallowAddingFields_SameAsOnIntake()
      {
         //arrange
         _config.ExcludeItemsMissingPrefix = false;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int) : new ItemDef(ItemType.String);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = rec =>
         {
            rec.AddItem("Fld7", "blah");
            return rec;
         };
         _config.AllowTransformToAlterFields = false;   //this setting drives this test (note that the test verifies FieldsInUse)
         _config.DeferTransformation = DeferTransformation.NotDeferred;
         _config.DeferOutput = DeferOutput.Indefinitely;  //so that Output won't steal resulting clusters

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         var flds = ((TransformProvider)(new PrivateAccessor(orchestrator)).GetField("_transformProvider")).FieldsInUse;

         //assert
         flds.Count.Should().Be(6);
         flds[0].Should().Be("Fld1");
         flds[1].Should().Be("Fld2");
         flds[2].Should().Be("Fld3");
         flds[3].Should().Be("Fld4");
         flds[4].Should().Be("Fld5");
         flds[5].Should().Be("Fld6");
         // _resulting clusters not used in this test
      }


      [Fact]
      public void FieldsInUseOnTransform_AllowAddingButNoFieldsAdded_SameFieldsInUseAsIntake()
      {
         //arrange
         _config.ExcludeItemsMissingPrefix = true;
         //no type definitions (everything string)
         _config.RetainQuotes = true;
         _config.AllowOnTheFlyInputFields = true;
         _config.AllowTransformToAlterFields = true;
         _config.DeferTransformation = DeferTransformation.NotDeferred;
         _config.DeferOutput = DeferOutput.Indefinitely;  //so that Output won't steal resulting clusters

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         var flds = ((TransformProvider)(new PrivateAccessor(orchestrator)).GetField("_transformProvider")).FieldsInUse;

         //assert
         flds.Count.Should().Be(6);
         flds[0].Should().Be("Fld1");
         flds[1].Should().Be("Fld2");
         flds[2].Should().Be("Fld3");
         flds[3].Should().Be("Fld4");
         flds[4].Should().Be("Fld5");
         flds[5].Should().Be("Fld6");
         // _resulting clusters not used in this test
      }


      [Fact]
      public void FieldsInUseOnTransform_AllowAddingFields_FieldsAdded()
      {
         //arrange
         _config.ExcludeItemsMissingPrefix = false;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; };  //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.Recordbound;
         _config.RecordboundTransformer = rec =>
         {
            rec.AddItem("Fld7", "blah");
            if (rec.RecNo == 3) rec.AddItem("Fld8", "blahblah");
            return rec;
         };
         _config.AllowTransformToAlterFields = true;   //this setting drives this test (note that the test verifies FieldsInUse)
         _config.DeferTransformation = DeferTransformation.NotDeferred;
         _config.DeferOutput = DeferOutput.Indefinitely;  //so that Output won't steal resulting clusters

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         var flds = ((TransformProvider)(new PrivateAccessor(orchestrator)).GetField("_transformProvider")).FieldsInUse;

         //assert
         flds.Count.Should().Be(8);
         flds[0].Should().Be("Fld1");
         flds[1].Should().Be("Fld2");
         flds[2].Should().Be("Fld3");
         flds[3].Should().Be("Fld4");
         flds[4].Should().Be("Fld7");  //note that when Fld7 is added to the record for the first time (record #1), Fld5 & Fld6 are not yet known (they appear in record #3)
         flds[5].Should().Be("Fld5");
         flds[6].Should().Be("Fld6");
         flds[7].Should().Be("Fld8");  //Fld8 OTOH is only added to record #3, i.e. when Fld5 & Fld6 are already on the list
         // _resulting clusters not used in this test
      }

   }
}
