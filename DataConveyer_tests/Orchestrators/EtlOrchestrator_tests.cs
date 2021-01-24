//EtlOrchestrator_tests.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace DataConveyer_tests.Orchestrators
{
   [TestClass]
   public class EtlOrchestrator_tests
   {

      [TestMethod]
      public void CreateTypeDefinitions_SimpleValues_CorrectData()
      {
         //arrange
         //   fld1 & fld3 DateTime, fld2 bool, everything else Int
         var explicitDefs = "fld1|D,fld2|B,fld3|D|mm/dd/yy";
         Func<string, ItemDef> typeDefiner = fn => new ItemDef(ItemType.Int, "000");
         var orchestrator = new EtlOrchestrator(new OrchestratorConfig());
         var orchestratorPO = new PrivateObject(orchestrator);

         //act
         var typeDefs = (TypeDefinitions)orchestratorPO.Invoke("CreateTypeDefinitions", new object[] { explicitDefs, typeDefiner });

         //assert
         typeDefs.GetFldType("fld1").Should().Be(ItemType.DateTime);
         typeDefs.GetFldFormat("fld1").Should().Be(string.Empty);  //if format missing, empty string is assumed
         typeDefs.GetFldParser("fld1").Should().BeOfType(typeof(Func<string, object>));
         typeDefs.GetFldParser("fld1")("invaliddate").Should().BeOfType(typeof(DateTime));
         typeDefs.GetFldParser("fld1")("invaliddate").Should().Be(default(DateTime));

         typeDefs.GetFldType("fld2").Should().Be(ItemType.Bool);
         typeDefs.GetFldFormat("fld2").Should().Be(string.Empty);
         typeDefs.GetFldParser("fld2").Should().BeOfType(typeof(Func<string, object>));
         typeDefs.GetFldParser("fld2")("TRUE").Should().BeOfType(typeof(bool));
         typeDefs.GetFldParser("fld2")("TRUE").Should().Be(true);

         typeDefs.GetFldType("fld3").Should().Be(ItemType.DateTime);
         typeDefs.GetFldFormat("fld3").Should().Be("mm/dd/yy");

         typeDefs.GetFldType("new1").Should().Be(ItemType.Int);
         typeDefs.GetFldFormat("new2").Should().Be("000");
         typeDefs.GetFldParser("new3")(" 0014 ").Should().Be(14);
      }


      [TestMethod]
      public void GetEtlOrchestrator_Defaults_OrchestratorConstructed()
      {
         //This test verifies the OrchestratorCreator class (factory)

         //arrange
         var config = new OrchestratorConfig();
         config.IntakeSupplier = gc => null;  //dummy
         config.OutputConsumer = (t, gc) => { };  //dummy

         //act
         var orchestrator = OrchestratorCreator.GetEtlOrchestrator(config);
         var orchestratorPO = new PrivateObject(orchestrator);

         //assert
         orchestrator.Should().BeOfType<EtlOrchestrator>();

         orchestratorPO.GetField("_intakeProvider").Should().BeOfType<Mavidian.DataConveyer.Intake.RawIntakeProvider>();  //default
         orchestratorPO.GetField("_transformProvider").Should().BeOfType<Mavidian.DataConveyer.Transform.RecordboundTransformProvider>(); //default
         orchestratorPO.GetField("_outputProvider").Should().BeOfType<Mavidian.DataConveyer.Output.RawOutputProvider>(); //default
      }


      [TestMethod]
      public void Ctor_DefaultBufferSize_UnboundedCapacities()
      {
         //arrange
         var config = new OrchestratorConfig();
         config.IntakeSupplier = gc => null;  //dummy
         config.IntakeBufferFactor = 2.0;
         config.TransformBufferFactor = 0.6;
         config.OutputBufferFactor = 3.0;
         config.OutputConsumer = (t, gc) => { };  //dummy

         //act
         var orchestrator = new EtlOrchestrator(config);
         var orchestratorPO = new PrivateObject(orchestrator);

         //assert
         orchestratorPO.GetField("_intakeBufferSize").Should().Be(-1);
         orchestratorPO.GetField("_transformInBufferSize").Should().Be(-1);
         orchestratorPO.GetField("_transformOutBufferSize").Should().Be(-1);
         orchestratorPO.GetField("_outputBufferSize").Should().Be(-1);
      }


      [TestMethod]
      public void Ctor_BufferSizesSet_CorrectCapacities()
      {
         //arrange
         var config = new OrchestratorConfig();
         config.IntakeSupplier = gc => null;  //dummy
         config.BufferSize = 4;
         config.IntakeBufferFactor = 2.0;
         config.TransformBufferFactor = 0.6;
         config.OutputBufferFactor = 2.65;
         config.OutputConsumer = (t, gc) => { };  //dummy

         //act
         var orchestrator = new EtlOrchestrator(config);
         var orchestratorPO = new PrivateObject(orchestrator);

         //assert
         orchestratorPO.GetField("_intakeBufferSize").Should().Be(8);
         orchestratorPO.GetField("_transformInBufferSize").Should().Be(4);
         orchestratorPO.GetField("_transformOutBufferSize").Should().Be(2);
         orchestratorPO.GetField("_outputBufferSize").Should().Be(11);
      }

   }
}

