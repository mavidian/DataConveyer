//EtlOrchestrator_tests_FieldsToUse.cs
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
using Mavidian.DataConveyer.Orchestrators;
using Mavidian.DataConveyer.Output;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace DataConveyer.Tests.Orchestrators_Output
{
   public class EtlOrchestrator_tests_FieldsToUse
   {
      private readonly OrchestratorConfig _config;

      private IEnumerable<string> _intakeLines()
      {
         yield return "YEAR  MONTH NUMERIC DATA";
         yield return "1966  12    43004840";
      }

      public EtlOrchestrator_tests_FieldsToUse()
      {
         _config = new OrchestratorConfig
         {
            InputDataKind = KindOfTextData.Flat
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.HeadersInFirstInputRow = true;
         _config.InputFields = "InFld1|4,InFld2|4,InFld3|12";
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.ClusterFilterPredicate = c => true;  // no transformations, data passed as is
         _config.SetOutputConsumer((string l) => { } );  //throwaay consumer, these tests do not evaluate output
      }


      [Fact]
      public void ProcessPipeline_OutputFieldsInConfig_FieldsToUseFromConfig()
      {
         //arrange
         _config.OutputFields = "OutFLd1|6,OutFld2|14";

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);

         //act
         _ = orchestrator.ExecuteAsync().Result;
         var fieldsToUse = ((OutputProvider)orchestratorPA.GetField("_outputProvider")).FieldsToUse;

         //assert
         fieldsToUse.Should().HaveCount(2);
         fieldsToUse[0].Should().Be("OutFLd1");
         fieldsToUse[1].Should().Be("OutFld2");
      }


      [Fact]
      public void ProcessPipeline_InFieldsButNotOutInConfig_FieldsToUseFromInFields()
      {
         //arrange

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);

         //act
         _ = orchestrator.ExecuteAsync().Result;
         var fieldsToUse = ((OutputProvider)orchestratorPA.GetField("_outputProvider")).FieldsToUse;

         //assert
         fieldsToUse.Count().Should().Be(3);
         fieldsToUse[0].Should().Be("InFld1");
         fieldsToUse[1].Should().Be("InFld2");
         fieldsToUse[2].Should().Be("InFld3");
      }


      [Fact]
      public void ProcessPipeline_NoInOrOutFieldsInConfig_FieldsToUseFromHeaderRow()
      {
         //arrange
         _config.InputFields = "|4,|7,|13";   //field widths are still needed here (FF intake), but field names are not specified, so they will be taken from header row
         _config.TrimInputValues = true;      //needed to remove spaces from field names

         var orchestrator = new EtlOrchestrator(_config);
         var orchestratorPA = new PrivateAccessor(orchestrator);

         //act
         _ = orchestrator.ExecuteAsync().Result;
         var fieldsToUse = ((OutputProvider)orchestratorPA.GetField("_outputProvider")).FieldsToUse;

         //assert
         fieldsToUse.Count().Should().Be(3);
         fieldsToUse[0].Should().Be("YEAR");
         fieldsToUse[1].Should().Be("MONTH");
         fieldsToUse[2].Should().Be("NUMERIC DATA");
      }
   }
}
