//SimpleOutputConsumer_tests.cs
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


using FluentAssertions;
using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DataConveyer_tests.Output
{
   /// <summary>
   /// These are in fact integration tests (of the entire pipeline)
   /// </summary>
   [TestClass]
      public class SimpleOutputConsumer_tests
      {
         OrchestratorConfig _config;

      int _inPtr = 0;
      List<Tuple<ExternalLine, int>> _inLines;

      private Tuple<ExternalLine, int> _inLine(IGlobalCache gc)
      {
         if (_inPtr >= _inLines.Count) return null;
         return _inLines[_inPtr++];
      }

      private IEnumerable<string> _intakeLines()
      {
         yield return "Line 01";
         yield return "Line 02";
         yield return "Line 03";
         yield return "Line 04";
         yield return "Line 05";
         yield return "Line 06";
         yield return "Line 07";
         yield return "Line 08";
      }

      //Result of the tests are held here:
      List<string> _resultingLines;  //container of the test results


      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig();

         //prepare extraction of the results from the pipeline
         _resultingLines = new List<string>();

         var sn = 0; //to assign sourceNo in a round-robin fashion: 1,2,3,1,2,3,1,2
         _inLines = _intakeLines().Select(l => l.ToExternalTuple(sn++ % 3 + 1)).ToList();
      }


      [TestMethod]
      public void processEntirePipeline_OutputConsumer_TargetAlways1()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Raw;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputDataKind = KindOfTextData.Raw;
         _config.OutputConsumer = (t, gc) => { _resultingLines.Add(t?.Item1.Text); };  // goes to single target regardless of t.Item2, i.e. targetNo

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(8);
         counts.ClustersRead.Should().Be(8);
         counts.RowsWritten.Should().Be(8);
         counts.ClustersWritten.Should().Be(8);

         _resultingLines.Count.Should().Be(9);   //includes end-of-data mark
         _resultingLines[0].Should().Be("Line 01");
         _resultingLines[1].Should().Be("Line 02");
         _resultingLines[2].Should().Be("Line 03");
         _resultingLines[3].Should().Be("Line 04");
         _resultingLines[4].Should().Be("Line 05");
         _resultingLines[5].Should().Be("Line 06");
         _resultingLines[6].Should().Be("Line 07");
         _resultingLines[7].Should().Be("Line 08");
         _resultingLines[8].Should().BeNull();
      }


      [TestMethod]
      public void processEntirePipeline_SimpleConsumer_TargetAlways1()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Raw;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputDataKind = KindOfTextData.Raw;
         _config.SetOutputConsumer(l => _resultingLines.Add(l));
         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(8);
         counts.ClustersRead.Should().Be(8);
         counts.RowsWritten.Should().Be(8);
         counts.ClustersWritten.Should().Be(8);

         _resultingLines.Count.Should().Be(9);   //includes end-of-data mark
         _resultingLines[0].Should().Be("Line 01");
         _resultingLines[1].Should().Be("Line 02");
         _resultingLines[2].Should().Be("Line 03");
         _resultingLines[3].Should().Be("Line 04");
         _resultingLines[4].Should().Be("Line 05");
         _resultingLines[5].Should().Be("Line 06");
         _resultingLines[6].Should().Be("Line 07");
         _resultingLines[7].Should().Be("Line 08");
         _resultingLines[8].Should().BeNull();
      }


      [TestMethod]
      public void processEntirePipeline_AsyncOutputConsumer_TargetAlways1()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Raw;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputDataKind = KindOfTextData.Raw;
         _config.AsyncOutput = true;
         _config.AsyncOutputConsumer = (t, gc) => { _resultingLines.Add(t?.Item1.Text); return Task.FromResult(0); };  // goes to single target regardless of t.Item2, i.e. targetNo

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(8);
         counts.ClustersRead.Should().Be(8);
         counts.RowsWritten.Should().Be(8);
         counts.ClustersWritten.Should().Be(8);

         _resultingLines.Count.Should().Be(9);   //includes end-of-data mark
         _resultingLines[0].Should().Be("Line 01");
         _resultingLines[1].Should().Be("Line 02");
         _resultingLines[2].Should().Be("Line 03");
         _resultingLines[3].Should().Be("Line 04");
         _resultingLines[4].Should().Be("Line 05");
         _resultingLines[5].Should().Be("Line 06");
         _resultingLines[6].Should().Be("Line 07");
         _resultingLines[7].Should().Be("Line 08");
         _resultingLines[8].Should().BeNull();
      }


      [TestMethod]
      public void processEntirePipeline_SimpleAsyncConsumer_TargetAlways1()
      {
         //arrange
         _config.InputDataKind = KindOfTextData.Raw;
         _config.IntakeSupplier = _inLine;  //has SourceNo assigned in a round-robin fashion
         //default (no) transformation
         _config.RouterType = RouterType.SourceToTarget;
         _config.OutputDataKind = KindOfTextData.Raw;
         _config.AsyncOutput = true;
         _config.SetAsyncOutputConsumer(l => { _resultingLines.Add(l); return Task.FromResult(0); });

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var counts = orchestrator.ExecuteAsync().Result;

         //assert
         counts.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         counts.RowsRead.Should().Be(8);
         counts.ClustersRead.Should().Be(8);
         counts.RowsWritten.Should().Be(8);
         counts.ClustersWritten.Should().Be(8);

         _resultingLines.Count.Should().Be(9);   //includes end-of-data mark
         _resultingLines[0].Should().Be("Line 01");
         _resultingLines[1].Should().Be("Line 02");
         _resultingLines[2].Should().Be("Line 03");
         _resultingLines[3].Should().Be("Line 04");
         _resultingLines[4].Should().Be("Line 05");
         _resultingLines[5].Should().Be("Line 06");
         _resultingLines[6].Should().Be("Line 07");
         _resultingLines[7].Should().Be("Line 08");
         _resultingLines[8].Should().BeNull();
      }

   }
}
