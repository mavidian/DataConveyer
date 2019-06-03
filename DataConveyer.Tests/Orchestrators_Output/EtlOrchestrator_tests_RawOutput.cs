//EtlOrchestrator_tests_RawOutput.cs
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
   public class EtlOrchestrator_tests_RawOutput
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
         yield return "\"DTP*348*D8*20001220\""; //surrounding quotes
         yield return "  LX*1     ";  //note 2 leading and 5 trailing spaces
         yield return "NM1*P3*1*FREDRICKSON*STEVE****XX*1234567891*25";
         yield return "SE*17*0001";
         yield return "GE*1*1421";
         yield return "IEA*1*455321165";
      }

      private IEnumerable<string> _intakeLinesMF()
      {
         yield return "@pRECTYPE=XYZ,@pNAME=Mary,@pNUM=123";
         yield return "@pRECTYPE=ABCD,@pABCD_ID=\"XYZ00883   \",@pNAME=Mary,@pNUM=223";   //spaces after 883 will be trailing
         yield return "@pRECTYPE=ABCD,@pABCD_ID=XYZ00883,@pNAME=\"Susan   \",@pNUM=323";  //note 3 spaces after Susan
         yield return "@pRECTYPE=ABCD,@pABCD_ID=\"XYZ00883   \",@pNAME=Mary,@pNUM=423,@pSpaces=\"  \"";  //even more trailing spaces
         yield return "EOF";
      }

      //Results of the tests are held here:
      private readonly List<string> _resultingLines;

      public EtlOrchestrator_tests_RawOutput()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Raw
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return true; };  //single record clusters
         _config.AllowOnTheFlyInputFields = true;        // to allow fields to showing trailing spaces
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.OutputDataKind = KindOfTextData.Raw;
         _config.SetOutputConsumer(l => { if (l != null) _resultingLines.Add(l); });

         _resultingLines = new List<string>();
      }


      [Fact]
      public void ProcessPipeline_RawOutputSimpleConfig_CorrectData()
      {
         //arrange
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(21);
         counts.ClustersRead.Should().Be(21);
         counts.ClustersWritten.Should().Be(21);
         counts.RowsWritten.Should().Be(21);

         _resultingLines.Should().HaveCount(21);

         _resultingLines[0].Should().Be("ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~");
         _resultingLines[1].Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS");
         _resultingLines[2].Should().Be("ST*834*0001");
         _resultingLines[3].Should().Be("BGN*00*1234*20001227*0838*PT***2");
         _resultingLines[4].Should().Be("N1*P5**FI*954529603");
         _resultingLines[11].Should().Be("N3*\"123 MAIN STREET\"");   //quotes respected
         _resultingLines[15].Should().Be("\"DTP*348*D8*20001220\"");   //quotes respected
         _resultingLines[16].Should().Be("  LX*1     ");   //leading/trailing spaces respected
         _resultingLines[20].Should().Be("IEA*1*455321165");
      }



      [Fact]
      public void ProcessPipeline_RawOutputLeaderTrailer_CorrectData()
      {
         //arrange
         _config.LeaderContents = "Start of sample raw data:  ";
         _config.TrailerContents = "---\r\nEnd of sample raw data:  ";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(21);
         counts.ClustersRead.Should().Be(21);
         counts.ClustersWritten.Should().Be(21);
         counts.RowsWritten.Should().Be(24);  // 21 + 1 (leader) + 2 (trailer)

         _resultingLines.Should().HaveCount(24);

         _resultingLines[0].Should().Be("Start of sample raw data:  ");
         _resultingLines[1].Should().Be("ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~");
         _resultingLines[2].Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS");
         _resultingLines[3].Should().Be("ST*834*0001");
         _resultingLines[4].Should().Be("BGN*00*1234*20001227*0838*PT***2");
         _resultingLines[5].Should().Be("N1*P5**FI*954529603");
         _resultingLines[12].Should().Be("N3*\"123 MAIN STREET\"");   //quotes respected
         _resultingLines[16].Should().Be("\"DTP*348*D8*20001220\"");   //quotes respected
         _resultingLines[17].Should().Be("  LX*1     ");   //leading/trailing spaces respected
         _resultingLines[21].Should().Be("IEA*1*455321165");
         _resultingLines[22].Should().Be("---");
         _resultingLines[23].Should().Be("End of sample raw data:  ");
      }


      [Fact]
      public void ProcessPipeline_RawOutputClustered_CorrectData()
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return recCnt % 4 == 0; };  // 4 record clusters
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(21);
         counts.ClustersRead.Should().Be(6);
         counts.ClustersWritten.Should().Be(6);
         counts.RowsWritten.Should().Be(21);

         _resultingLines.Should().HaveCount(21);

         _resultingLines[0].Should().Be("ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~");
         _resultingLines[1].Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS");
         _resultingLines[2].Should().Be("ST*834*0001");
         _resultingLines[3].Should().Be("BGN*00*1234*20001227*0838*PT***2");
         _resultingLines[4].Should().Be("N1*P5**FI*954529603");
         _resultingLines[11].Should().Be("N3*\"123 MAIN STREET\"");   //quotes respected
         _resultingLines[15].Should().Be("\"DTP*348*D8*20001220\"");   //quotes respected
         _resultingLines[16].Should().Be("  LX*1     ");   //leading/trailing spaces respected
         _resultingLines[20].Should().Be("IEA*1*455321165");
      }


      [Fact]
      public void ProcessPipeline_RawOutputClusteredPlusDistractors_DataNotDistracted()
      {
         //arrange
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return recCnt % 4 == 0; };  // 4 record clusters
         _config.MarkerStartsCluster = false;  //predicate matches the last record in cluster
         _config.HeadersInFirstOutputRow = true;
         _config.QuotationMode = QuotationMode.Always;
         _config.TrimOutputValues = true;
         _config.OutputKeyPrefix = "XYZ";
         _config.ArbitraryOutputDefs = new string[] { "blah" };
         _config.DefaultOutputFieldWidth = 45;
         _config.ExcludeExtraneousFields = true;

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(21);
         counts.ClustersRead.Should().Be(6);
         counts.ClustersWritten.Should().Be(6);
         counts.RowsWritten.Should().Be(21);

         _resultingLines.Should().HaveCount(21);

         _resultingLines[0].Should().Be("ISA*00*          *00*          *01*054318936      *01*123456789      *020801*0900*U*00501*00000012 *0*T*~");
         _resultingLines[1].Should().Be("GS*PO*4405197800*999999999*20101127*1719*1421*X*004010VICS");
         _resultingLines[2].Should().Be("ST*834*0001");
         _resultingLines[3].Should().Be("BGN*00*1234*20001227*0838*PT***2");
         _resultingLines[4].Should().Be("N1*P5**FI*954529603");
         _resultingLines[11].Should().Be("N3*\"123 MAIN STREET\"");   //quotes respected
         _resultingLines[15].Should().Be("\"DTP*348*D8*20001220\"");   //quotes respected
         _resultingLines[16].Should().Be("  LX*1     ");   //leading/trailing spaces respected
         _resultingLines[20].Should().Be("IEA*1*455321165");
      }


      [Fact]
      public void ProcessPipeline_RawOutputMultiItems_ItemsMerged()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLinesMF()).StringSupplier);
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("XYZMary123");
         _resultingLines[1].Should().Be("ABCDXYZ00883   Mary223");
         _resultingLines[2].Should().Be("ABCDXYZ00883Susan   323");
         _resultingLines[3].Should().Be("ABCDXYZ00883   Mary423  ");  //trailing spaces respected
         _resultingLines[4].Should().Be(string.Empty);                //EOF, no value
      }


      [Fact]
      public void ProcessPipeline_RawOutputFieldsSpecified_OnlySelectedItemsMerged()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Keyword;
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLinesMF()).StringSupplier);
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.OutputFields = "RECTYPE,ABCD_ID,NAME";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.RowsRead.Should().Be(5);
         counts.ClustersRead.Should().Be(5);
         counts.ClustersWritten.Should().Be(5);
         counts.RowsWritten.Should().Be(5);

         _resultingLines.Should().HaveCount(5);

         _resultingLines[0].Should().Be("XYZMary");
         _resultingLines[1].Should().Be("ABCDXYZ00883   Mary");
         _resultingLines[2].Should().Be("ABCDXYZ00883Susan   ");
         _resultingLines[3].Should().Be("ABCDXYZ00883   Mary");
         _resultingLines[4].Should().Be(string.Empty);  //EOF, no value for any of the specified keys
      }

   }
}
