//Program.cs  (TextFileHandler project)
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


using Mavidian.DataConveyer.Common;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace TextFileHandler
{
   static class Program
   {
      static void Main(string[] args)
      {
         var asmName = System.Reflection.Assembly.GetExecutingAssembly().GetName();
         Console.WriteLine("{0} v{1} started execution on {2:MM-dd-yyyy a\\t hh:mm:ss tt}",
                            asmName.Name,
                            asmName.Version,
                            DateTime.Now);
         Console.WriteLine("DataConveyer library used: " + ProductInfo.CurrentInfo.ToString());
         // args[0] = fully qualified name of the keyword input file
         string inputKwFile = args[0];
         // args[1] = fully qualified name of the delimited input file
         string inputCsvFile = args[1];
         // args[2] = fully qualified name of the fixed width input file
         string inputFixedFile = args[2];
         // args[3] = fully qualified name of the arbitrary input file
         string inputArbitraryFile = args[3];
         // args[4] = fully qualified name of the first raw input file to merge
         string input1stFileToMerge = args[4];
         // args[5] = fully qualified name of the second raw input file to merge
         string input2ndFileToMerge = args[5];
         // args[6] = fully qualified name of the raw input file to split
         string inputFileToSplit = args[6];
         // args[7] = fully qualified name of the X12 input file
         string inputX12File = args[7];
         // args[8] = fully qualified name of the XML input file
         string inputXmlFile = args[8];
         // args[9] = fully qualified name of the JSON input file
         string inputJsonFile = args[9];
         // args[9] = fully qualified name of the flattened JSON input file (for unbound JSON)
         string inputFlattenedJsonFile = args[10];

         // args[10] - args[19] = fully qualified names of the 4 corresponding output files
         string outputFile1 = args[11];
         string outputFile2 = args[12];
         string outputFile3 = args[13];
         string outputFile4 = args[14];
         string outputFile5 = args[15];
         string outputFile6 = args[16];
         string outputFile7 = args[17];
         string outputFile8 = args[18];
         string outputFile9 = args[19];
         string outputFile10 = args[20];
         string outputFile11 = args[21];
         string outputFile12 = args[22];

         Console.WriteLine("Input keyword file: {0}", inputKwFile);
         Console.WriteLine("Input delimited file: {0}", inputCsvFile);
         Console.WriteLine("Input fixed width file: {0}", inputFixedFile);
         Console.WriteLine("Input arbitrary file: {0}", inputArbitraryFile);
         Console.WriteLine("Input raw files to merge: {0} and {1}", input1stFileToMerge, input2ndFileToMerge);
         Console.WriteLine("Input raw file to split: {0}", inputFileToSplit);
         Console.WriteLine("Input X12 file: {0}", inputX12File);
         Console.WriteLine("Input XML file: {0}", inputXmlFile);
         Console.WriteLine(" 1st output file: {0}", outputFile1);
         Console.WriteLine(" 2nd output file: {0}", outputFile2);
         Console.WriteLine(" 3rd output file: {0}", outputFile3);
         Console.WriteLine(" 4th output file: {0}", outputFile4);
         Console.WriteLine(" 5th output file: {0}", outputFile5);
         Console.WriteLine(" 6th output file: {0}", outputFile6);
         Console.WriteLine(" 7th output file: {0}", outputFile7);
         Console.WriteLine(" 8th output file: {0}", outputFile8);
         Console.WriteLine(" 9th output file: {0}", outputFile9); //XML
         Console.WriteLine("10th output file: {0}", outputFile10);  //JSON
         Console.WriteLine();

         var stopWatch = new Stopwatch();
         OrchestratorConfig config;
         ProcessResult processResult;
         IOrchestrator orchestrator;
         int[] progressCounts;
         Task<ProcessResult> executionTask;

         //Part 1: process sample keyword file

         Console.WriteLine();
         Console.WriteLine("Part 1: Processing a sample keyword file...");
         Console.WriteLine();

         stopWatch.Start();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 1: process sample keyword file", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 1,
            InputDataKind = KindOfTextData.Keyword,
            InputFileName = inputKwFile,
            HeadersInFirstInputRow = false,
            RetainQuotes = false,
            InputKeyPrefix = "@p",
            ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem,
            ExplicitTypeDefinitions = null,
            //TypeDefiner = key => { return key.EndsWith("_DT") ? new ItemDef(ItemType.DateTime, null)
            //                                                   : new ItemDef(ItemType.String, null); }, //fields ending with _DT are dates, everything else String
            ClusterMarker = (rec, prevRec, recCnt) => { return (string)rec["_Class"] == "CMC_APPREC_HDR_EXTERNAL"; },  //records having @p_Class=CMC_APPREC_HDR_EXTERNAL denote start of the cluster
            MarkerStartsCluster = true,  //predicate matches the first record in cluster
            AllowOnTheFlyInputFields = true,
            TransformerType = TransformerType.Recordbound,
            //RecordboundTransformer = r => throw new DivideByZeroException("simulated exception"),
            AllowTransformToAlterFields = false,
            ConcurrencyLevel = 1,
            //AsyncOutput = true,
            OutputDataKind =  KindOfTextData.Delimited,
            OutputFileName = outputFile1,
            HeadersInFirstOutputRow = true,
            OutputKeyPrefix = "@p",
            TrimOutputValues = true,
            QuotationMode = QuotationMode.Always,
            ExcludeExtraneousFields = true,
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         orchestrator = OrchestratorCreator.GetEtlOrchestrator(config);

         //Main processing occurs here:
         executionTask = orchestrator.ExecuteAsync();
         processResult = executionTask.Result;
         orchestrator.Dispose();

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of keyword file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total rows read from the input file:    {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows: {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:     {0}", processResult.ClustersWritten);
         Console.WriteLine("Total rows sent to the output file:     {0}", processResult.RowsWritten);
         Console.WriteLine();


         //Part 2: process sample delimited file (CSV)

         Console.WriteLine();
         Console.WriteLine("Part 2: Processing a sample delimited file (CSV)...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 2: process sample delimited file (CSV)", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 10,
            InputDataKind = KindOfTextData.Delimited,
            InputFileNames = inputCsvFile,
            HeadersInFirstInputRow = true,
            RetainQuotes = false,
            InputKeyPrefix = "",
            ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem,
            ExplicitTypeDefinitions = null,
            ClusterMarker = (rec, prevRec, recCnt) => { return recCnt % 2 == 0; },  //split every other record
            MarkerStartsCluster = false,  //predicate matches the last record in cluster
            AllowOnTheFlyInputFields = true,
            TransformerType = TransformerType.Recordbound,
            RecordboundTransformer = rec => rec,  //simply let all records go through
            AllowTransformToAlterFields = true,  //not needed, but verifies Transformer ability to rebuild field list
            //ConcurrencyLevel = 4,
            //AsyncOutput = true,
            OutputDataKind = KindOfTextData.Keyword,
            OutputFileNames = outputFile2,
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //orchestrator.Dispose();  //irrational(!) - testing only
            //Main processing occurs here:
            processResult = orchestrator.ExecuteAsync().Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of delimited file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total rows read from the input file:    {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows: {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:     {0}", processResult.ClustersWritten);
         Console.WriteLine("Total rows sent to the output file:     {0}", processResult.RowsWritten);
         Console.WriteLine();


         //Part 3: process sample flat file

         Console.WriteLine();
         Console.WriteLine("Part 3: Processing a sample flat file...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 3: process sample flat file", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 1000,
            InputDataKind = KindOfTextData.Flat,
            InputFileName = inputFixedFile,
            //AsyncIntake = true,
            BufferSize = 100,
            IntakeBufferFactor = 1.0,
            OutputBufferFactor = 1.0,
            HeadersInFirstInputRow = false,
            RetainQuotes = false,
            InputKeyPrefix = "",
            ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem,
            ExplicitTypeDefinitions = null,
            InputFields = "Provider|10,State|2,County|5,DRG|5,Cases|4,Median Payment|8",
            TransformerType = TransformerType.ClusterFilter,
            ClusterFilterPredicate = clstr => true,  //simply let all records go through
            AllowTransformToAlterFields = true,  //not needed, but verifies Transformer ability to rebuild field list
            ConcurrencyLevel = 1,
            OutputDataKind = KindOfTextData.Keyword,
            //AsyncOutput = true,
            OutputFileNames = outputFile3,
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //orchestrator.Dispose();  //irrational(!) - testing only
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            Thread.Sleep(1000);
            orchestrator.CancelExecution();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of flat file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total rows read from the input file:    {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows: {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:     {0}", processResult.ClustersWritten);
         Console.WriteLine("Total rows sent to the output file:     {0}", processResult.RowsWritten);
         Console.WriteLine();


         //Part 4: process sample arbitrary file (EDI)

         Console.WriteLine();
         Console.WriteLine("Part 4: Processing a sample arbitrary file (EDI)...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 4: process sample arbitrary file (EDI)", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 1,
            InputDataKind = KindOfTextData.Arbitrary,
            InputFileNames = inputArbitraryFile,
            BufferSize = 100,
            IntakeBufferFactor = 1.0,
            OutputBufferFactor = 1.0,
            TrimInputValues = true,
            ArbitraryInputDefs = new string[] { "Segment ^[^*]*", @"ISA06 (?<=^ISA\*([^*]*\*){5})([^*]*)" },
            TransformerType = TransformerType.ClusterFilter,
            ClusterFilterPredicate = clstr => {
               return clstr[0].GetItem("Segment").StringValue == "ISA";
            },  //only allow ISA through
            ConcurrencyLevel = 1,
            OutputDataKind = KindOfTextData.Arbitrary,
            OutputFileName = outputFile4,
            ArbitraryOutputDefs = new string[] { "Interchange envelope contains a sender ID of '{ISA06}'" },
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of arbitrary file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total rows read from the input file:    {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows: {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:     {0}", processResult.ClustersWritten);
         Console.WriteLine("Total rows sent to the output file:     {0}", processResult.RowsWritten);
         Console.WriteLine();


         //Part 5: process two sample raw files to be merged

         Console.WriteLine();
         Console.WriteLine("Part 5: Processing two sample raw file to be merged...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig()
         {
            ReportProgress = true,
            ProgressInterval = 1,
            InputFileNames = input1stFileToMerge + "|" + input2ndFileToMerge,
            BufferSize = 100,
            IntakeBufferFactor = 1.0,
            OutputBufferFactor = 1.0,
            OutputFileNames = outputFile5,
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of two raw files completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total rows read from the input file:    {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows: {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:     {0}", processResult.ClustersWritten);
         Console.WriteLine("Total rows sent to the output file:     {0}", processResult.RowsWritten);
         Console.WriteLine();


         //Part 6: process a sample raw file to be split

         Console.WriteLine();
         Console.WriteLine("Part 6: Processing a sample raw file to be split...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig()
         {
            ReportProgress = true,
            ProgressInterval = 1,
            InputFileNames = inputFileToSplit,
            BufferSize = 100,
            IntakeBufferFactor = 1.0,
            OutputBufferFactor = 1.0,
            RouterType = RouterType.PerRecord,
            RecordRouter = (r, c) => ((string)r[0]).StartsWith("6") ? 1 : 2,  //if 1st char is 6 goto output6, otherwise goto output7
            OutputFileNames = outputFile6 + "|" + outputFile7,
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of raw file to split completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total rows read from the input file:    {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows: {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:     {0}", processResult.ClustersWritten);
         Console.WriteLine("Total rows sent to the output file:     {0}", processResult.RowsWritten);
         Console.WriteLine();



         //Part 7: process sample X12 files (2 files: same as arbitrary + another X12)

         Console.WriteLine();
         Console.WriteLine("Part 7: Processing sample X12 files (2 files: same as arbitrary + another X12)...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 7: process sample X12 files (2 files: same as arbitrary + another X12)", LogEntrySeverity.Information))
         {
            DefaultX12FieldDelimiter = '+',
            DefaultX12SegmentDelimiter = "&\r\n",
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 1,
            InputDataKind = KindOfTextData.X12,
            //InputFileName = inputArbitraryFile,
            //InputFileName = inputX12File,
            //InputFileNames = inputArbitraryFile + "|" + inputX12File,
            InputFileNames = inputX12File + "|" + inputArbitraryFile,
            BufferSize = 100,
            IntakeBufferFactor = 1.0,
            OutputBufferFactor = 1.0,
            TrimInputValues = true,
            ClusterMarker = (rec, prevRec, recCnt) => { return new string[] { "ISA", "GS", "ST", "GE", "IEA" }.Contains(rec["Segment"]); },  //each transaction is own cluster (also single envelope marking segments)
            MarkerStartsCluster = true,  //predicate matches the first record in cluster
            TransformerType = TransformerType.ClusterFilter,
            ClusterFilterPredicate = clstr => true,  //let all clusters through
            //ClusterFilterPredicate = clstr => {
            //   return clstr[0].GetItem("Segment").StringValue == "ISA";
            //},  //only allow ISA through
            ConcurrencyLevel = 1,
            OutputDataKind = KindOfTextData.X12,
            OutputFileName = outputFile8,
            ArbitraryOutputDefs = new string[] { "Interchange envelope contains a sender ID of '{Elem006}'" },
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of X12 files completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total segments read from the input file: {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows:  {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:      {0}", processResult.ClustersWritten);
         Console.WriteLine("Total segments sent to the output file:  {0}", processResult.RowsWritten);
         Console.WriteLine();



         //Part 8: process a sample XML file

         Console.WriteLine();
         Console.WriteLine("Part 8: Processing a sample XML file...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 8: process a sample XML file", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 1,
            InputDataKind = KindOfTextData.XML,
            InputFileName = inputXmlFile,
            AllowOnTheFlyInputFields = true,
            XmlJsonIntakeSettings = "CollectionNode|dataset,RecordNode|record",
            BufferSize = 10,
            ClusterMarker = (rec, prevRec, recCnt) => recCnt == 50,  //50 records in a cluster
            MarkerStartsCluster = false,
            TransformerType = TransformerType.Clusterbound,
            ClusterboundTransformer = inClstr =>
            {  //pipe-delimited list of last names, 50 at a time
               var outClstr = inClstr.GetEmptyClone();
               var outRec = inClstr[0].GetEmptyClone();
               outRec.AddItem("FiftyNames", inClstr.Records.Select(r => (string)r["LName"]).Aggregate((a, n) => a + "|" + n));
               outClstr.AddRecord(outRec);
               return outClstr;
            },
            AllowTransformToAlterFields = true,
            ConcurrencyLevel = 8,
            OutputDataKind = KindOfTextData.Keyword,
            OutputFileName = outputFile9,
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of XML file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total records read from the input file:  {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows:  {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:      {0}", processResult.ClustersWritten);
         Console.WriteLine("Total records sent to the output file:   {0}", processResult.RowsWritten);
         Console.WriteLine();



         //Part 9: create an XML file from a sample delimited file

         Console.WriteLine();
         Console.WriteLine("Part 9: Creating an XML file from a sample delimited file (from Part 2)...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 9: create an XML file from a sample delimited file", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 10,
            InputDataKind = KindOfTextData.Delimited,
            InputFileNames = inputCsvFile,
            HeadersInFirstInputRow = true,
            RetainQuotes = false,
            InputKeyPrefix = "",
            ActionOnDuplicateKey = ActionOnDuplicateKey.IgnoreItem,
            ExplicitTypeDefinitions = null,
            AllowOnTheFlyInputFields = true,
            TransformerType = TransformerType.RecordFilter,
            RecordFilterPredicate = rec => rec["state"] as string == "OH",
            AllowTransformToAlterFields = true,  //not needed, but verifies Transformer ability to rebuild field list
            ConcurrencyLevel = 4,
            AsyncOutput = true,
            OutputDataKind = KindOfTextData.XML,
            OutputFileName = outputFile10,
            XmlJsonOutputSettings = "CollectionNode|OhioResidents,RecordNode|Person,AttributeFields|first_name;last_name,IndentChars|  ",  //pretty-print
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of a file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total records read from the input file:  {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows:  {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:      {0}", processResult.ClustersWritten);
         Console.WriteLine("Total records sent to the output file:   {0}", processResult.RowsWritten);
         Console.WriteLine();



         //Part 10: process and then create a JSON file

         //Read a JSON file containing people sorted by last name, create another JSON file containing just names (combined first&last)
         // of people with clustering by last name initial
         // inputJsonFile -> outputFile10

         Console.WriteLine();
         Console.WriteLine("Part 10: Transforming sorted JSON file into another JSON file...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 10: process and then create a JSON file", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 10,
            InputDataKind = KindOfTextData.JSON,
            InputFileNames = inputJsonFile,
            XmlJsonIntakeSettings = "CollectionNode|,RecordNode|",  //matches SQL Server FOR JSON output
            ExplicitTypeDefinitions = "BIRTH_DATE|D",  //note that field types are separate from JSON types on input (but if they match, no type conversions will occur)
            ClusterMarker = (rec, prevRec, recCnt) =>
            {  //group records by last name initial
               if (prevRec == null) return true;
               return ((string)rec["LAST_NAME"])[0] != ((string)prevRec["LAST_NAME"])[0];
            },
            MarkerStartsCluster = true,
            AllowOnTheFlyInputFields = true,
            TransformerType = TransformerType.Recordbound,
            RecordboundTransformer = rec =>
            {  //transform each record to contain ID, FULL_NAME and AGE
               var outRec = rec.GetEmptyClone();
               outRec.AddItem("ID", rec["ID"]);
               outRec.AddItem("FULL_NAME", rec["FIRST_NAME"] + " " + rec["LAST_NAME"]);
               var dob = (DateTime)rec["BIRTH_DATE"]; var now = DateTime.Now;
               outRec.AddItem("AGE", now.Year - dob.Year - (now.DayOfYear < dob.DayOfYear ? 1 : 0));
               return outRec;
            },
            AllowTransformToAlterFields = true,
            ConcurrencyLevel = 4,
            AsyncOutput = true,
            OutputDataKind = KindOfTextData.JSON,
            OutputFileName = outputFile11,
            XmlJsonOutputSettings = "ClusterNode|,RecordNode|,IndentChars|  ",  //pretty-print, nested arrays representing clusters and records
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of a file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total records read from the input file:  {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows:  {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:      {0}", processResult.ClustersWritten);
         Console.WriteLine("Total records sent to the output file:   {0}", processResult.RowsWritten);
         Console.WriteLine();



         //Part 11: process and then create a hierarchical JSON file

         //Read a JSON file containing data on population, driver and vehicle numbers by US states for years 2009-2020.
         // On input, the data is sorted by state & year, but not grouped (i.e. a flat JSON array of objects).
         // On output, data is grouped by state & year forming hierarchical JSON consisting of an array of State objects, each
         // containing Year objects with the 3 number properties (population, drivers and vehicles respectively).
         // In addition, numbers (which are strings on input) are output as JSON numbers.
         // inputFlattenedJsonFile -> outputFile12

         Console.WriteLine();
         Console.WriteLine("Part 11: Grouping flattened JSON file into hierarchical JSON file...");
         Console.WriteLine();

         stopWatch.Restart();

         progressCounts = new int[] { 0, 0, 0 };  //intake, transformation, output

         config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Part 11: process and create a JSON file (hierarchy-aware unbound JSON)", LogEntrySeverity.Information))
         {
            //CloseLoggerOnDispose = false,
            ReportProgress = true,
            ProgressInterval = 10,
            InputDataKind = KindOfTextData.UnboundJSON,
            InputFileNames = inputFlattenedJsonFile,
            XmlJsonIntakeSettings = "CollectionNode|,RecordNode|",  //matches SQL Server FOR JSON output
             ClusterMarker = (rec, prevRec, recCnt) =>
            {  //group records by State
               return (prevRec == null) || ((string)rec["State"]) != ((string)prevRec["State"]);
            },
            MarkerStartsCluster = true,
            AllowOnTheFlyInputFields = true,
            TransformerType = TransformerType.Clusterbound,
            ClusterboundTransformer = clstr =>
            {  //transform each cluster, so that record field names reflect the required JSON hierarchy
               var outClstr = clstr.GetEmptyClone();
               //There will be a single record for an output cluster that combines PDV data for all years
               var firstRec = clstr.Records[0];
               string state = (string)firstRec["State"];
               var outRec = firstRec.GetEmptyClone();
               foreach (var rec in clstr.Records) //each rec represents Year
               {
                  Debug.Assert(state == (string)rec["State"]); //records got clustered by State
                  var year = (string)rec["Year"];
                  outRec.AddItem($"{state}.{year}.Population", (string)rec["Population"]);
                  outRec.AddItem($"{state}.{year}.Drivers", (string)rec["Drivers"]);
                  outRec.AddItem($"{state}.{year}.Vehicles", (string)rec["Vehicles"]);
                  //Note that we could have the above numbers cast to int instead of string, but this would require them to be of int type, i.e. the following config setting:
                  // ExplicitTypeDefinitions = "Population|I,Drivers|I,Vehicles|I",
               }
               outClstr.AddRecord(outRec);

               //Note: As an alternative, instead of sending a single record that combines all Years for a cluster (State),
               //      you could send multiple records (one per Year). Like so:
               //foreach (var rec in clstr.Records) //each rec represents Year
               //{
               //   Debug.Assert(state == (string)rec["State"]); //records got clustered by State
               //   var year = (string)rec["Year"];
               //   var outRec = rec.GetEmptyClone();
               //   outRec.AddItem($"{state}.{year}.Population", (string)rec["Population"]);
               //   outRec.AddItem($"{state}.{year}.Drivers", (string)rec["Drivers"]);
               //   outRec.AddItem($"{state}.{year}.Vehicles", (string)rec["Vehicles"]);
               //   outClstr.AddRecord(outRec);
               //}

               return outClstr;
            },
            AllowTransformToAlterFields = true,
            ConcurrencyLevel = 4,
            AsyncOutput = true,
            OutputDataKind = KindOfTextData.UnboundJSON,
            OutputFileName = outputFile12,
            XmlJsonOutputSettings = "ClusterNode|,RecordNode|,IndentChars|  ",  //pretty-print, nested arrays representing clusters and records
            PhaseStartingHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} starting"); },
            PhaseFinishedHandler = (s, e) => { Console.WriteLine($"{DateTime.Now.ToString():HH:mm:ss.fff} {e.Phase.ToString()} finished ({e.ClstrCnt.ToString()} clusters processed)"); },
            ProgressChangedHandler = (s, e) =>
            {
               if (e.Phase == Phase.Intake) progressCounts[0]++; else if (e.Phase == Phase.Transformation) progressCounts[1]++; else progressCounts[2]++;
            }
         };

         using (orchestrator = OrchestratorCreator.GetEtlOrchestrator(config))
         {
            //Main processing occurs here:
            executionTask = orchestrator.ExecuteAsync();
            processResult = executionTask.Result;
         }

         //Report execution statistics:
         Console.WriteLine();
         Console.WriteLine($"Counts of ProgressChanged calls by phase: {progressCounts[0].ToString()}, {progressCounts[1].ToString()}, {progressCounts[2].ToString()}");
         stopWatch.Stop();
         Console.WriteLine();
         Console.WriteLine("Execution of a file completed in {0} seconds", stopWatch.Elapsed.TotalSeconds.ToString("##,##0.000"));
         Console.WriteLine("Completion status is {0}", processResult.CompletionStatus);
         Console.WriteLine("Total records read from the input file:  {0}", processResult.RowsRead);
         Console.WriteLine("Total clusters created from input rows:  {0}", processResult.ClustersRead);
         Console.WriteLine("Total clusters processed on output:      {0}", processResult.ClustersWritten);
         Console.WriteLine("Total records sent to the output file:   {0}", processResult.RowsWritten);
         Console.WriteLine();



         Console.WriteLine("PROCESSING COMPLETE.");
         Console.ReadLine();
      }
   }
}
