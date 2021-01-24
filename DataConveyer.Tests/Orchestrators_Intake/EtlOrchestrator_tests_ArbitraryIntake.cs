//EtlOrchestrator_tests_ArbitraryIntake.cs
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

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_ArbitraryIntake
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~";
         yield return "GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS";
         yield return "ST*834*0001";
         yield return "BGN*00*1234*20001227*0838*PT***2";
         yield return "N1*P5**FI*954529603";
         yield return "INS*Y*18*021*28*A*E**FT";
         yield return "REF*0F*123456789";
         yield return "REF*1L*G86W553";
         yield return "DTP*356*D8*20001220";
         yield return "NM1*IL*1*STEPHENS*MARIE*V***34*123456789";
         yield return "PER*IP**HP*4152296748*WP*4152968732";
         yield return "N3*\"123 MAIN STREET\"";  //to test RetainQuotes
         yield return "N4*SAN FRANCISCO*CA*94515";
         yield return "DMG*D8*19691017*F*M";
         yield return "HD*021**HLT*01A3*EMP";
         yield return "DTP*348*D8*20001220";
         yield return "LX*1";
         yield return "NM1*P3*1*FREDRICKSON*STEVE****XX*1234567891*25";
         yield return "SE*17*0001";
         yield return "GE*1*1421";
         yield return "IEA*1*455321165";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_ArbitraryIntake()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Arbitrary
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         //no type definitions (everything string)
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //each record is its own cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster
         _config.ArbitraryInputDefs = new string[] { "Segment ^[^*]*", @"ISA06 (?<=^ISA\*([^*]*\*){5})([^*]*)", @"N301 (?<=^N3\*)([^*]*)" };
         _config.DeferTransformation = DeferTransformation.Indefinitely;  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessPipeline_ArbtiraryIntakeSimpleConfig_CorrectData()
      {
         //arrange
         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(21);

         resultingClusters[0].Count.Should().Be(1);
         resultingClusters[1].Count.Should().Be(1);
         resultingClusters[20].Count.Should().Be(1);

         var aRec = resultingClusters[0][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("ISA");
         aRec["ISA06"].Should().Be("054318936      ");
         aRec["N301"].Should().Be(string.Empty);

         aRec = resultingClusters[1][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("GS");
         aRec["ISA06"].Should().Be(string.Empty);
         aRec["N301"].Should().Be(string.Empty);
         aRec["dummy"].Should().BeNull();

         aRec = resultingClusters[11][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("N3");
         aRec["ISA06"].Should().Be(string.Empty);
         aRec["N301"].Should().Be("123 MAIN STREET");
         aRec["dummy"].Should().BeNull();

         aRec = resultingClusters[20][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("IEA");
         aRec["ISA06"].Should().Be(string.Empty);
         aRec["N301"].Should().Be(string.Empty);
         aRec["dummy"].Should().BeNull();
      }


      [Fact]
      public void ProcessPipeline_ArbtiraryIntakeQuoteAndTrim_CorrectData()
      {
         //arrange

         _config.RetainQuotes = true;
         _config.TrimInputValues = true;

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(21);

         resultingClusters.Count.Should().Be(21);
         resultingClusters[0].Count.Should().Be(1);
         resultingClusters[1].Count.Should().Be(1);
         resultingClusters[20].Count.Should().Be(1);

         var aRec = resultingClusters[0][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("ISA");
         aRec["ISA06"].Should().Be("054318936");
         aRec["N301"].Should().Be(string.Empty);

         aRec = resultingClusters[1][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("GS");
         aRec["ISA06"].Should().Be(string.Empty);
         aRec["N301"].Should().Be(string.Empty);
         aRec["dummy"].Should().BeNull();

         aRec = resultingClusters[11][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("N3");
         aRec["ISA06"].Should().Be(string.Empty);
         aRec["N301"].Should().Be("\"123 MAIN STREET\"");
         aRec["dummy"].Should().BeNull();

         aRec = resultingClusters[20][0];
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("IEA");
         aRec["ISA06"].Should().Be(string.Empty);
         aRec["N301"].Should().Be(string.Empty);
         aRec["dummy"].Should().BeNull();
      }

   }
}
