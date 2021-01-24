//EtlOrchestrator_tests_X12Intake.cs
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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks.Dataflow;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Intake
{
   public class EtlOrchestrator_tests_X12Intake
   {
      private readonly OrchestratorConfig _config;
      private IEnumerable<string> _intakeLines()  //note 2 ISA envelopes with different delimiters
      {
         yield return "ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~\r";  //here, segment terminator is \r
                                                                                                                                      //  yield return "ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~";  //here, no segment terminator, so default (\r\n) gets assumed
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
         yield return "ISA>00>          >01>SECRET    >ZZ>SUBMITTERS.ID  >ZZ>RECEIVERS.ID   >030101>1253>^>00501>000000905>1>T>:";
         yield return "GS>HC>SENDER CODE>RECEIVER CODE>19991231>0802>1>X>005010X222";
         yield return "ST>837>0021>005010X222";
         yield return "BHT>0019>00>244579>20061015>1023>CH";
         yield return "NM1>41>2>PREMIER BILLING SERVICE>>>>>46>TGJ23";
         yield return "PER>IC>JERRY>TE>3055552222>EX>231";
         yield return "NM1>40>2>KEY INSURANCE COMPANY>>>>>46>66783JJT";
         yield return "HL>1>>20>1";
         yield return "PRV>BI>PXC>203BF0100Y";
         yield return "NM1>85>2>BEN KILDARE SERVICE>>>>>XX>9876543210";
         yield return "N3>234 SEAWAY ST";
         yield return "N4>MIAMI>FL>33111";
         yield return "REF>EI>587654321";
         yield return "NM1>87>2";
         yield return "N3>2345 OCEAN BLVD";
         yield return "N4>MAIMI>FL>33111";
         yield return "HL>2>1>22>1";
         yield return "SBR>P>>2222-SJ>>>>>>CI";
         yield return "NM1>IL>1>SMITH>JANE>>>>MI>JS00111223333";
         yield return "DMG>D8>19430501>F";
         yield return "NM1>PR>2>KEY INSURANCE COMPANY>>>>>PI>999996666";
         yield return "REF>G2>KA6663";
         yield return "HL>3>2>23>0";
         yield return "PAT>19";
         yield return "NM1>QC>1>SMITH>TED";
         yield return "N3>236 N MAIN ST";
         yield return "N4>MIAMI>FL>33413";
         yield return "DMG>D8>19730501>M";
         yield return "CLM>26463774>100>>>11:B:1>Y>A>Y>I";
         yield return "REF>D9>17312345600006351";
         yield return "HI>BK:0340>BF:V7389";
         yield return "LX>1";
         yield return "SV1>HC:99213>40>UN>1>>>1";
         yield return "DTP>472>D8>20061003";
         yield return "LX>2";
         yield return "SV1>HC:87070>15>UN>1>>>1";
         yield return "DTP>472>D8>20061003";
         yield return "LX>3";
         yield return "SV1>HC:99214>35>UN>1>>>2";
         yield return "DTP>472>D8>20061010";
         yield return "LX>4";
         yield return "SV1>HC:86663>10>UN>1>>>2";
         yield return "DTP>472>D8>20061010";
         yield return "SE>42>0021";
         yield return "GE>1>1";
         yield return "IEA>1>000000905";
      }

      //Results of the tests are held here:
      private readonly ConcurrentQueue<KeyValCluster> _resultingClusters;  //container of the test results
      private readonly ActionBlock<KeyValCluster> _resultsExtractor;  //block to load results to container

      public EtlOrchestrator_tests_X12Intake()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.X12
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.DeferTransformation = DeferTransformation.Indefinitely;  //to prevent linking clusteringBlock to transformingBlock (which could steal clusters from results extractor)

         _resultingClusters = new ConcurrentQueue<KeyValCluster>();
         _resultsExtractor = new ActionBlock<KeyValCluster>(c => _resultingClusters.Enqueue(c));
      }


      [Fact]
      public void ProcessPipeline_X12Intake_CorrectData()
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return new string[] { "ISA", "GS", "ST", "GE", "IEA" }.Contains(rec["Segment"]); };  //each transaction is own cluster (also single envelope marking segments)
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster

         var orchestrator = TestUtilities.GetTestOrchestrator(_config, "_clusteringBlock", _resultsExtractor);

         //act
         _ = orchestrator.ExecuteAsync();
         _resultsExtractor.Completion.Wait();

         //assert
         var resultingClusters = _resultingClusters.ToList();

         resultingClusters.Should().HaveCount(10);
         resultingClusters[0].Count.Should().Be(1);  //ISA
         resultingClusters[1].Count.Should().Be(1);  //GS
         resultingClusters[2].Count.Should().Be(17); //ST
         resultingClusters[3].Count.Should().Be(1);  //GE
         resultingClusters[4].Count.Should().Be(1);  //IEA
         resultingClusters[5].Count.Should().Be(1);  //ISA
         resultingClusters[6].Count.Should().Be(1);  //GS
         resultingClusters[7].Count.Should().Be(42); //ST
         resultingClusters[8].Count.Should().Be(1);  //GE
         resultingClusters[9].Count.Should().Be(1);  //IEA

         var aRec = resultingClusters[0][0]; //ISA
         aRec.Count.Should().Be(17);  //17 fields in ISA
         aRec["Segment"].Should().Be("ISA");
         aRec[0].Should().Be("ISA");
         aRec["Elem006"].Should().Be("054318936      ");
         aRec[6].Should().Be("054318936      ");
         aRec["Elem016"].Should().Be("~");
         aRec[16].Should().Be("~");

         aRec = resultingClusters[1][0];
         aRec.Count.Should().Be(9);
         aRec["Segment"].Should().Be("GS");
         aRec["Elem008"].Should().Be("004010VICS");
         aRec[8].Should().Be("004010VICS");

         var aClstr = resultingClusters[2];  //ST - entire transaction
         aClstr.Count.Should().Be(17);  //17 segments from ST to SE
         aRec = aClstr[0];  //ST
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("ST");
         aRec[0].Should().Be("ST");
         aRec = aClstr[1];  //BGN
         aRec.Count.Should().Be(9);
         aRec["Segment"].Should().Be("BGN");
         aRec[0].Should().Be("BGN");
         aRec = aClstr[15];  //NM1
         aRec.Count.Should().Be(11);
         aRec["Segment"].Should().Be("NM1");
         aRec[0].Should().Be("NM1");
         aRec = aClstr[16];  //SE
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("SE");
         aRec[0].Should().Be("SE");
         aRec["Elem002"].Should().Be("0001");
         aRec[2].Should().Be("0001");

         aRec = resultingClusters[3][0];
         aRec["Segment"].Should().Be("GE");

         aRec = resultingClusters[4][0];
         aRec["Segment"].Should().Be("IEA");

         aRec = resultingClusters[5][0];
         aRec["Segment"].Should().Be("ISA");
         aRec.Count.Should().Be(17);  //17 fields in ISA
         aRec["Elem016"].Should().Be(":");

         aRec = resultingClusters[6][0];
         aRec["Segment"].Should().Be("GS");
         aRec.Count.Should().Be(9);
         aRec["Elem001"].Should().Be("HC");
         aRec["Elem008"].Should().Be("005010X222");

         aClstr = resultingClusters[7];  //ST - entire transaction
         aClstr.Count.Should().Be(42);  //42 segments from ST to SE
         aRec = aClstr[0];  //ST
         aRec.Count.Should().Be(4);
         aRec["Segment"].Should().Be("ST");
         aRec[0].Should().Be("ST");
         aRec = aClstr[1];  //BHT
         aRec.Count.Should().Be(7);
         aRec["Segment"].Should().Be("BHT");
         aRec["Elem006"].Should().Be("CH");
         aRec = aClstr[38];  //LX
         aRec.Count.Should().Be(2);
         aRec["Segment"].Should().Be("LX");
         aRec["Elem001"].Should().Be("4");
         aRec = aClstr[41];  //SE
         aRec.Count.Should().Be(3);
         aRec["Segment"].Should().Be("SE");
         aRec["Elem001"].Should().Be("42");

         aRec = resultingClusters[8][0];
         aRec["Segment"].Should().Be("GE");
         aRec.Count.Should().Be(3);
         aRec["Elem001"].Should().Be("1");
         aRec["Elem002"].Should().Be("1");

         aRec = resultingClusters[9][0];
         aRec["Segment"].Should().Be("IEA");
         aRec.Count.Should().Be(3);
         aRec["Elem001"].Should().Be("1");
         aRec["Elem002"].Should().Be("000000905");
      }

   }
}
