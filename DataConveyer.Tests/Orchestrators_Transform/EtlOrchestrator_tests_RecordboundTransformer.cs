//EtlOrchestrator_tests_RecordboundTransformer.cs
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
using Mavidian.DataConveyer.Orchestrators;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Transform
{
   public class EtlOrchestrator_tests_RecordboundTransformer
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=223";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Susan,@pNUM=323";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=423";
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=523";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Mary,@pNUM=623";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Joan,@pNUM=723";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Jane,@pNUM=823";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=Cindy,@pNUM=923";
         yield return "EOF";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_RecordboundTransformer()
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
         _config.TransformerType = TransformerType.Recordbound;
         _config.DeferOutput = DeferOutput.Indefinitely;  //so that Output won't steal resulting clusters

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessPipeline_PassUnchangedRecords_SameDataAsInput()
      {
         //arrange
         _config.RecordboundTransformer = rec => rec;  // pass the same record to output

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);
         resultingClusters[0].Count.Should().Be(4);
         resultingClusters[1].Count.Should().Be(6);

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["NAME"].Should().Be("Mary");
         kvRec["NUM"].Should().Be(123);

         kvRec = resultingClusters[0][2];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Susan");
         kvRec["NUM"].Should().Be(323);

         kvRec = resultingClusters[1][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["NUM"].Should().Be(523);

         kvRec = resultingClusters[1][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Mary");
         kvRec["NUM"].Should().Be(623);

         kvRec = resultingClusters[1][4];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Cindy");
         kvRec["NUM"].Should().Be(923);

         kvRec = resultingClusters[1][5];
         kvRec.Count.Should().Be(1);
         kvRec["RECTYPE"].Should().BeNull();  //non existing
         kvRec.GetItem("RECTYPE").ItemDef.Type.Should().Be(ItemType.Void);
         kvRec["EOF"].Should().BeNull();  //existing w/no value (BTW, note the prefix mismatch)
         kvRec.GetItem("EOF").ItemDef.Type.Should().Be(ItemType.String);
      }


      [Fact]
      public void ProcessPipeline_PassNothing_RecordsFilteredOut()
      {
         //arrange
         _config.RecordboundTransformer = rec => null;  // pass nothing to output; null records will be filtered out

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         _resultingClusters.Should().BeEmpty();     //null records filtered out, resulting in clusters with no records - also filtered out
      }


      [Fact]
      public void ProcessPipeline_PassNothingConditionally_RecordsFilteredOut()
      {
         //arrange

         _config.RecordboundTransformer = rec => rec.ClstrNo == 1 ? null : rec;  // the 4 records from 1st cluster filtered out

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Count.Should().Be(1);     //null records filtered out, resulting in clusters with no records - also filtered out
         resultingClusters[0].Count.Should().Be(6);  //6 null records in only cluster (from 2nd intake clstr and the 1st was filtered out)

         var kvRec = resultingClusters[0][0];
         kvRec.Count.Should().Be(3);
         kvRec["RECTYPE"].Should().Be("XYZ");
         kvRec["NUM"].Should().Be(523);

         kvRec = resultingClusters[0][1];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Mary");
         kvRec["NUM"].Should().Be(623);

         kvRec = resultingClusters[0][4];
         kvRec.Count.Should().Be(4);
         kvRec["RECTYPE"].Should().Be("ABCD");
         kvRec["NAME"].Should().Be("Cindy");
         kvRec["NUM"].Should().Be(923);
      }


      [Fact]
      public void ProcessPipeline_IncreaseNumBy20_ValuesIncreased()
      {
         //arrange
         _config.RecordboundTransformer = rec => IncreaseNumBy20(rec);  // reject every cluster

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_holdingBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(2);     //same as orig
         resultingClusters[0].Count.Should().Be(4);  //same as orig
         resultingClusters[1].Count.Should().Be(6);  //6 orig + 2 extra records

         resultingClusters[0][0]["NUM"].Should().Be(143);  //increased by 20
         resultingClusters[0][2]["NUM"].Should().Be(343);  //increased by 20
         resultingClusters[0][2]["NAME"].Should().Be("Susan");

         resultingClusters[1][0]["NUM"].Should().Be(543);  //increased by 20
         resultingClusters[1][2]["NUM"].Should().Be(743);  //increased by 20
         resultingClusters[1][2]["NAME"].Should().Be("Joan");
         resultingClusters[1][4]["NUM"].Should().Be(943);  //increased by 20
         resultingClusters[1][5]["NUM"].Should().BeNull();  //EOF (no NUM)

      }
      /// <summary>
      /// Helper function to increase the value of NUM element in the record by 20
      /// </summary>
      /// <param name="record"></param>
      /// <returns>A copy of the original record with NUM values increased by 20</returns>
      private KeyValRecord IncreaseNumBy20(IRecord record)
      {
         dynamic recToReturn = record.GetClone();  //need to clone in order to manipulate

         if (recToReturn.ContainsKey("NUM"))
         {
            int newNum = (int)recToReturn["NUM"] + 20;
            recToReturn.NUM = newNum;
         }
         return recToReturn;
      }


   }
}
