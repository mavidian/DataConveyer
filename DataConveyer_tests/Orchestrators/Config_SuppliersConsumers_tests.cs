//Config_SuppliersConsumers_tests.cs
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
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;

namespace DataConveyer_tests.Orchestrators
{
   [TestClass]
   public class Config_SuppliersConsumers_tests
   {


      [TestMethod]
      public void Process_NoFilesSuppliersOrConsumers_DefaultsAssumed()
      {
         //arrange
         var config = new OrchestratorConfig();
         var orchestrator = new EtlOrchestrator(config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);  //this confirms that default supplier and consumer were used
         result.RowsRead.Should().Be(0);
         result.ClustersRead.Should().Be(0);
         result.RowsWritten.Should().Be(0);
         result.ClustersWritten.Should().Be(0);
      }


      [TestMethod]
      public void Process_IntakeFileAndSupplierPresent_SupplierWins()
      {
         //arrange
         int inCnt = 0;

         var config = new OrchestratorConfig();
         config.InputFileNames = "C:\\non-existing-file.abc";
         //config.IntakeTextSupplier = () => inCnt++ < 5 ? $"Line #{inCnt}" : null;
         config.TextIntakeSupplier = () =>
         {
            return inCnt++ < 5 ? $"Line #{inCnt}" : null;
         };

         var orchestrator = new EtlOrchestrator(config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);  //note that InputFileNames would've caused InitializationError
         result.RowsRead.Should().Be(5);
         result.ClustersRead.Should().Be(5);
         result.RowsWritten.Should().Be(5);
         result.ClustersWritten.Should().Be(5);
         inCnt.Should().Be(6);
      }


      [TestMethod]
      public void Process_OutputFileAndConsumererPresent_ConsumerWins()
      {
         //arrange
         int inCnt = 0;
         var outLines = new List<Tuple<ExternalLine,int>>();

         var config = new OrchestratorConfig();
         config.InputFileNames = "C:\\non-existing-file.abc";
         config.IntakeSupplier = gc => inCnt++ < 3 ? $"Line #{inCnt}".ToExternalTuple() : null;
         config.OutputConsumer = (tpl, gc) => outLines.Add(tpl);

         var orchestrator = new EtlOrchestrator(config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);  //note that either InputFileNames or OutputFileNames would've caused InitializationError
         result.RowsRead.Should().Be(3);
         result.ClustersRead.Should().Be(3);
         result.RowsWritten.Should().Be(3);
         result.ClustersWritten.Should().Be(3);
         inCnt.Should().Be(4);
         outLines.Count.Should().Be(4);  //incl. EOD, i.e. null
         outLines[0].Item1.Text.Should().Be("Line #1");
         outLines[0].Item2.Should().Be(1);
         outLines[2].Item1.Text.Should().Be("Line #3");
         outLines[2].Item2.Should().Be(1);
         outLines[3].Should().BeNull();
      }

   }
}

