//EtlOrchestrator_tests_ProcessResult.cs
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
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using Moq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using Xunit;

namespace DataConveyer.Tests.Orchestrators
{
   public class EtlOrchestrator_tests_ProcessResult
   {
      private readonly OrchestratorConfig _config;

      private readonly List<Tuple<LogEntrySeverity, string, LogEntry>> _fatalLogMsgs; //updated by mockFatalLogger (Item1=severity, Item2=message, Item3=entire entry sent to logger)

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


      //Result of the tests are held here:
      private readonly List<string> _resultingLines;  //container of the test results

      public EtlOrchestrator_tests_ProcessResult()
      {
         _fatalLogMsgs = new List<Tuple<LogEntrySeverity, string, LogEntry>>(); var mockFatalLogger = new Mock<ILogger>();  //records Fatal messages to _fatalLogMsgs (plus title box)
         mockFatalLogger.Setup(l => l.LoggingThreshold).Returns(LogEntrySeverity.Fatal);
         mockFatalLogger.Setup(l => l.Log(It.IsAny<LogEntry>()))
                        .Callback((LogEntry e) => { if (e.Severity <= LogEntrySeverity.Fatal) _fatalLogMsgs.Add(Tuple.Create(e.Severity, e.MessageOnDemand(), e )); });

         _config = new OrchestratorConfig(mockFatalLogger.Object)
         {
            InputDataKind = KindOfTextData.Keyword
         };
         _config.SetIntakeSupplier(new IntakeSupplierProvider(_intakeLines()).StringSupplier);
         _config.RetainQuotes = false;
         _config.InputKeyPrefix = "@p";
         _config.ExcludeItemsMissingPrefix = false;
         _config.ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem;
         _config.TypeDefiner = key => key == "NUM" ? new ItemDef(ItemType.Int, null) : new ItemDef(ItemType.String, null);  //NUM Int, everything else String
         _config.ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["RECTYPE"] == "XYZ"; }; //records having @pRECTYPE=XYZ denote start of the cluster
         _config.MarkerStartsCluster = true;  //predicate matches the first record in cluster 
         _config.AllowOnTheFlyInputFields = true;
         _config.TransformerType = TransformerType.ClusterFilter;
         _config.OutputConsumer = (t, gc) => _resultingLines.Add(t?.Item1.Text);   // place the lines on the list to be tested/asserted


         //prepare extraction of the results from the pipeline
         _resultingLines = new List<string>();
      }


      [Fact]
      public void ProcessPipeline_SimpleProcessNoCancel_IntakeDepletedAndCompleteOutput()
      {
         //arrange
         _config.ClusterFilterPredicate = clstr => { Thread.Sleep(10); return true; };  // accept every cluster, but wait 10ms to ease out cancellation

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.IntakeDepleted);
         result.RowsRead.Should().Be(10);
         result.ClustersRead.Should().Be(2);
         result.RowsWritten.Should().Be(10);
         result.ClustersWritten.Should().Be(2);

         _resultingLines.Count.Should().Be(11);  //10 + terminating null line
      }


      [Theory(Skip = "May fail due to non-determinism")]
      [Repeat(1000)]
      public void ProcessPipeline_SimpleProcessCancel_CanceledAndIncompleteOutput(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //This test is not exact, it attempts to cancel the pipelinie while the transformer (ClusterFilterPredicate) is running.
         //On MsTests, this test ocassionally (rarely) failed (Expected Canceled, but found IntakeDepleted).

         //arrange
         _config.ClusterFilterPredicate = clstr => { Thread.Sleep(5); return true; };  // accept every cluster, but wait 5ms to ease out cancellation
         _config.ConcurrencyLevel = 1;  //timings (delays in ClusterFilterTransformer) need to cumulate for successful test

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var task = orchestrator.ExecuteAsync();
         Thread.Sleep(10);
         orchestrator.CancelExecution();
         var result = task.Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.Canceled);
         result.RowsRead.Should().BeLessOrEqualTo(10);
         result.ClustersRead.Should().BeLessOrEqualTo(2);  //note there is no guarantee that the Intake phase will complete

         _resultingLines.Count.Should().BeLessThan(11);
      }


      [Theory(Skip = "May fail due to non-determinism")]
      [Repeat(1000)]
      public void ProcessPipeline_SimpleProcessTimeout_TimedOutAndIncompleteOutput(int iterationNumber, int totalRepeats)
      {
         var dummy = iterationNumber; // to clear xUnit1026 warning
         dummy = totalRepeats;

         //This test is not exact, it compares the amount of TimeLimit against the delay in the tranformer, i.e. ClusterFilterPredicate.
         //In xUnit, assuming transformer delay of 6ms, it consistently passes with TimeLimit values of 15ms or less.
         //On MsTests, this test ocassionally failed (Expected TimedOut, but found IntakeDepleted.) even with timeout of 10ms.

         //arrange
         _config.TimeLimit = TimeSpan.FromMilliseconds(10);  //, so that timeout occurs
         _config.ClusterFilterPredicate = clstr => { Thread.Sleep(6); return true; };  // accept every cluster, but wait 6ms to ease out cancellation
         _config.ConcurrencyLevel = 1;  //timings (delays in ClusterFilterPredicate) need to cumulate for successful test

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.TimedOut);
         result.RowsRead.Should().BeLessOrEqualTo(10);
         result.ClustersRead.Should().BeLessOrEqualTo(2);  //note there is no guarantee that the Intake phase will complete

         _resultingLines.Count.Should().BeLessThan(11);
      }


      [Fact]
      public void ProcessPipeline_IntakeLargerThanIntakeRecordLimit_LimitReached()
      {
         //arrange
         _config.IntakeRecordLimit = 6;
         _config.ClusterFilterPredicate = clstr => { return true; };  // accept every cluster; no delay for this test

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.LimitReached);
         result.RowsRead.Should().Be(6);
         result.ClustersRead.Should().Be(2);
         result.RowsWritten.Should().Be(6);
         result.ClustersWritten.Should().Be(2);

         _resultingLines.Count.Should().Be(7);  //6 (per IntakeRecordLimit setting) + terminating null line
      }


      [Fact]
      public void CreatePipeline_MissingInputFile_InitializationErrorNoOutputInitAttempted()
      {
         //arrange
         _config.IntakeSupplier = OrchestratorConfig.DefaultIntakeSupplier;  //to reset supplier set in ctor (and thus allow InputFileNames to take over)
         _config.OutputConsumer = OrchestratorConfig.DefaultOutputConsumer;  //to reset consumer set in ctor (and thus allow OutputFileNames to take over)
         _config.InputFileNames = "C:/non-existing_file.abc";
         _config.OutputFileNames = @"\\BadUNCpath";
         //Note that EagerInitialization is false by default!

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.InitializationError);
         result.RowsRead.Should().Be(0);
         result.ClustersRead.Should().Be(0);
         result.RowsWritten.Should().Be(0);
         result.ClustersWritten.Should().Be(0);

         _resultingLines.Count.Should().Be(0);  //nothing, the pipeline hasn't even started

         _fatalLogMsgs.Count.Should().Be(2);  //input file failed; hence output initialization wasn't attempted in (even though it would've also failed) - 1st entry is log title box
         _fatalLogMsgs[0].Item1.Should().Be(LogEntrySeverity.None);  //log title box
         var logRslt = _fatalLogMsgs[1];
         logRslt.Item1.Should().Be(LogEntrySeverity.Fatal);
         logRslt.Item2.Contains("Attempt to access input file(s) 'C:/non-existing_file.abc' failed").Should().BeTrue();  //text from IntakeProvider class
         logRslt.Item2.Contains("System.IO.FileNotFoundException").Should().BeTrue();
         logRslt.Item3.Exception.Should().BeOfType<FileNotFoundException>();
      }


      [Fact]
      public void CreatePipeline_MissingInputFileEagerInitialization_InitializationErrorsOutputInitAttempted()
      {
         //arrange
         _config.EagerInitialization = true;
         _config.IntakeSupplier = OrchestratorConfig.DefaultIntakeSupplier;  //to reset supplier set in ctor (and thus allow InputFileName to take over)
         _config.OutputConsumer = OrchestratorConfig.DefaultOutputConsumer;  //to reset consumer set in ctor (and thus allow OutputFileName to take over)
         _config.InputFileName = "C:/non-existing_file.abc";
         _config.OutputFileName = @"\\BadUNCpath";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.InitializationError);
         result.RowsRead.Should().Be(0);
         result.ClustersRead.Should().Be(0);
         result.RowsWritten.Should().Be(0);
         result.ClustersWritten.Should().Be(0);

         _resultingLines.Count.Should().Be(0);  //nothing, the pipeline hasn't even started

         _fatalLogMsgs.Count.Should().Be(3);  // output initialization is always attempted if EagerInitialization is set (plus log title box)
         _fatalLogMsgs[0].Item1.Should().Be(LogEntrySeverity.None);  //log title box
         var logRslt = _fatalLogMsgs[1];
         logRslt.Item1.Should().Be(LogEntrySeverity.Fatal);
         logRslt.Item2.Contains("Attempt to access input file(s) 'C:/non-existing_file.abc' failed").Should().BeTrue();  //text from IntakeProvider class
         logRslt.Item2.Contains("System.IO.FileNotFoundException").Should().BeTrue();
         logRslt.Item3.Exception.Should().BeOfType<FileNotFoundException>();
         logRslt = _fatalLogMsgs[2];
         logRslt.Item1.Should().Be(LogEntrySeverity.Fatal);
         logRslt.Item2.Contains("Attempt to create output file(s) '\\\\BadUNCpath' failed").Should().BeTrue();  //text from OutputProvider class; note escaped backslashes
         logRslt.Item2.Contains("Exception of type System.IO.IOException occurred: The specified path is invalid. : '\\\\BadUNCpath'").Should().BeTrue();
         logRslt.Item3.Exception.Should().BeOfType<IOException>();
      }


      [Fact]
      public void CreatePipeline_BadOutputFile_InitializationError()
      {
         //arrange
         _config.OutputConsumer = OrchestratorConfig.DefaultOutputConsumer;  //to reset consumer set in ctor (and thus allow OutputFileName to take over)
         _config.OutputFileNames = @"\\BadUNCpath";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         var result = orchestrator.ExecuteAsync().Result;

         //assert
         result.CompletionStatus.Should().Be(CompletionStatus.InitializationError);
         result.RowsRead.Should().Be(0);
         result.ClustersRead.Should().Be(0);
         result.RowsWritten.Should().Be(0);
         result.ClustersWritten.Should().Be(0);

         _resultingLines.Count.Should().Be(0);  //nothing, the pipeline hasn't even started

         _fatalLogMsgs.Count.Should().Be(2);    // intake init OK, but output init failed; first entry is log title box
         _fatalLogMsgs[0].Item1.Should().Be(LogEntrySeverity.None);  //log title box
         var logRslt = _fatalLogMsgs[1];
         logRslt.Item1.Should().Be(LogEntrySeverity.Fatal);
         logRslt.Item2.Contains("Attempt to create output file(s) '\\\\BadUNCpath' failed").Should().BeTrue();  //text from OutputProvider class; note escaped backslashes
         logRslt.Item2.Contains("Exception of type System.IO.IOException occurred: The specified path is invalid. : '\\\\BadUNCpath'").Should().BeTrue();
         logRslt.Item3.Exception.Should().BeOfType<IOException>();
      }


      [Fact]
      public void InitErrorOccurred_CalledMulitpleTimesMissingInputFile_InitIntakeDoneOnlyOnce()
      {
         //This test verifies setup of the IntakeProvider, which only calls InitIntake once, no matter how many times it's attempted (Lazy)
         //OutputProvider is set up the same way.

         //arrange
         _config.IntakeSupplier = OrchestratorConfig.DefaultIntakeSupplier;  //to reset supplier set in ctor (and thus allow InputFileNames to take over)
         _config.InputFileNames = "C:/non-existing_file.abc";

         var orchestrator = new EtlOrchestrator(_config);

         //act
         bool a = orchestrator.InitErrorOccurred;  //this should trigger InitIntake

         //assert
         a.Should().Be(true);
         _fatalLogMsgs.Count.Should().Be(2);  // logged failed InitIntake; first entry is log title box
         _fatalLogMsgs[0].Item1.Should().Be(LogEntrySeverity.None);  //log title box
         var logRslt = _fatalLogMsgs[1];
         logRslt.Item1.Should().Be(LogEntrySeverity.Fatal);
         logRslt.Item2.Contains("Attempt to access input file(s) 'C:/non-existing_file.abc' failed").Should().BeTrue();  //text from IntakeProvider class
         logRslt.Item2.Contains("System.IO.FileNotFoundException").Should().BeTrue();
         logRslt.Item3.Exception.Should().BeOfType<FileNotFoundException>();

         //act2
         bool b = orchestrator.InitErrorOccurred;          //this should not trigger InitIntake
         var result = orchestrator.ExecuteAsync().Result;  //neither should this

         //assert2
         result.CompletionStatus.Should().Be(CompletionStatus.InitializationError);
         result.RowsRead.Should().Be(0);
         result.ClustersRead.Should().Be(0);
         result.RowsWritten.Should().Be(0);
         result.ClustersWritten.Should().Be(0);

         _resultingLines.Count.Should().Be(0);  //nothing, the pipeline hasn't even started

         b.Should().Be(true);
         _fatalLogMsgs.Count.Should().Be(2);   //no new failures confirms no new attempt to call InitIntake
      }

   }
}
