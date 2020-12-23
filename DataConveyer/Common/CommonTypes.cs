//CommonTypes.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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


//Contents:
// enum Phase
// enum ExecutionState
// enum ActionOnDuplicateKey
// enum KindOfTextData
// enum TransformerType
// enum RouterType
// enum QuotationMode
// enum DeferTransformation
// enum DeferOutput
// enum OutputStartTiming
// enum PropertyBinAttachedTo

using Mavidian.DataConveyer.Orchestrators;
using System;

namespace Mavidian.DataConveyer.Common
{

   /// <summary>
   /// Phase of the Data Conveyer process execution.
   /// </summary>
   public enum Phase
   {
      /// <summary>
      /// Extract (E) phase of the ETL process - reading data from the sources.
      /// </summary>
      Intake,
      /// <summary>
      /// Transform (T) phase of the ETL process - data translation.
      /// </summary>
      Transformation,
      /// <summary>
      /// Load (L) phase of the ETL process - writing data to the targets.
      /// </summary>
      Output
   }


   /// <summary>
   /// Processing state of one of the three phases: Intake, Transformation or Output.
   /// </summary>
   public enum ExecutionState
   {
      /// <summary>
      /// Execution of the phase has not yet started. 
      /// </summary>
      NotYetStarted,
      /// <summary>
      /// Phase execution is in progress.
      /// </summary>
      Running,
      /// <summary>
      /// Phase execution has finished.
      /// </summary>
      Complete
   }

   /// <summary>
   /// Action in case duplicate key is encountered on intake record.
   /// </summary>
   public enum ActionOnDuplicateKey
   {
      /// <summary>
      /// The item (key-value pair) is excluded from the record (first item wins).
      /// </summary>
      IgnoreItem,
      /// <summary>
      /// The previous value for the key is replaced (last item wins).
      /// </summary>
      ReplaceItem,
      /// <summary>
      /// The entire intake record is excluded.
      /// </summary>
      ExcludeRecord,
      /// <summary>
      /// The duplicate key is substituted by a default key.
      /// </summary>
      AssignDefaultKey
   }


   /// <summary>
   /// Type (format) of text data to process (data lines are strings separated by CR/LF).
   /// </summary>
   public enum KindOfTextData
   {
      /// <summary>
      /// This is the default data kind where data is not parsed or formatted.
      /// On intake, the entire row contents is extracted into a single field named "RAW_REC".
      /// On output, if multiple fields are encountered, they are simply spliced together.
      /// 
      /// </summary>
      Raw,
      /// <summary>
      /// Keyword data, i.e. key-value pairs.
      /// </summary>
      Keyword,
     /// <summary>
     /// Values delimited by a given character, such as comma separated values.
     /// </summary>
      Delimited,
      /// <summary>
      /// Flat data, i.e. fixed width fields.
      /// </summary>
      Flat,
      /// <summary>
      /// Text data with fields "cherry-picked" using arbitrary formulas.
      /// </summary>
      Arbitrary,
      /// <summary>
      /// XML data (tabular).
      /// </summary>
      XML,
      /// <summary>
      ///  JSON data (tabular).
      /// </summary>
      JSON,
      /// <summary>
      ///  JSON data of unlimited hierarchy depth (subject to special field naming convention).
      /// </summary>
      UnboundJSON,
      /// <summary>
      /// An X12 document (EDI).
      /// </summary>
      X12,
      /// <summary>
      /// HL7 v2.x message(s) (future use).
      /// </summary>
      HL7,
      /// <summary>
      /// Text data that uses supplied functions to convert to/from canonical format (future use).
      /// </summary>
      Ultimate
   }


   /// <summary>
   /// Type of transformer to be invoked during Transform phase.
   /// </summary>
   public enum TransformerType
   {
      /// <summary>
      /// Each input cluster gets transformed into a sequence of output clusters (1: 0..many) based on the <see cref="OrchestratorConfig.UniversalTransformer"/> function.
      /// 
      /// </summary>
      Universal,
      /// <summary>
      /// Each input cluster gets transformed into a single output cluster (1:1) based on the <see cref="OrchestratorConfig.ClusterboundTransformer"/> function.
      /// </summary>
      Clusterbound,
      /// <summary>
      /// Every record in each input cluster gets transformed into a corresponding record in output cluster (1:1) based on the <see cref="OrchestratorConfig.RecordboundTransformer"/> function.
      /// </summary>
      Recordbound,
      /// <summary>
      /// No transformation, but each input cluster may be removed from output (1:0..1)  based on the <see cref="OrchestratorConfig.ClusterFilterPredicate"/> function.
      /// </summary>
      ClusterFilter,
      /// <summary>
      /// No transformation, but every record in each input cluster may be removed from output (1:0..1) based on the <see cref="OrchestratorConfig.RecordboundTransformer"/> function.
      /// </summary>
      RecordFilter,
      /// <summary>
      /// No transformation; instead, clusters are merged (reduced) into an aggregated cluster (1:1)   (future use).
      /// </summary>
      Aggregator
   }


   /// <summary>
   /// Type of router that determines the output target.
   /// </summary>
   public enum RouterType
   {
      /// <summary>
      /// All records are routed to the first output target (TargetNo = 1).
      /// </summary>
      SingleTarget,
      /// <summary>
      /// Each record is routed to the target that corresponds to the record's source (TargetNo = SourceNo).
      /// </summary>
      SourceToTarget,
      /// <summary>
      /// All records in a cluster are routed to the same output target based on the <see cref="OrchestratorConfig.ClusterRouter"/> function.
      /// </summary>
      PerCluster,
      /// <summary>
      /// Each record is routed individually based on the <see cref="OrchestratorConfig.RecordRouter"/> function.
      /// </summary>
      PerRecord
   }


   /// <summary>
   /// Specifies which values are to be surrounded with quotes on output. 
   /// </summary>
   public enum QuotationMode
   {
      /// <summary>
      /// Output values are not quoted, except for those that contain commas or quotes.
      /// </summary>
      OnlyIfNeeded,
      /// <summary>
      /// String and date values are quoted on output, while decimal or integer values are not (except if formatted to contain commas).
      /// </summary>
      StringsAndDates,
      /// <summary>
      /// All values are surrounded with quotes on output.
      /// </summary>
      Always
   }


   /// <summary>
   /// Timing when Data Conveyer is allowed to start the <see cref="Phase.Transformation"/> phase.
   /// </summary>
   public enum DeferTransformation
   {
      /// <summary>
      /// No deferral; the <see cref="Phase.Transformation"/> phase starts immediately upon beginning of processing.
      /// In this case, values returned by the <see cref="OrchestratorConfig.RecordInitiator"/> function are ignored by Data Conveyer.
      /// </summary>
      NotDeferred,
      /// <summary>
      /// Start of the <see cref="Phase.Transformation"/> phase until one of these 2 conditions, whichever comes first:
      /// <list type="bullet">
      /// <item>The <see cref="OrchestratorConfig.RecordInitiator"/> function (that Data Conveyer calls for each record read on intake) returns true.</item>
      /// <item>All records have been read from Intake.</item>
      /// </list>
      /// This is the only <see cref="DeferTransformation"/> setting where the return value from the <see cref="OrchestratorConfig.RecordInitiator"/> function is respected.
      /// </summary>
      UntilRecordInitiation,
      /// <summary>
      /// Start of the <see cref="Phase.Transformation"/> phase is deferred until completion of the entire Intake phase, including completion of record clustering.
      /// Note that in this case, values returned by the <see cref="OrchestratorConfig.RecordInitiator"/> function are ignored by Data Conveyer,
      /// i.e. true value(s) will not trigger the start of the Transformation phase.
      /// Use of this setting is not recommended except for troubleshooting situations.
      /// </summary>
      UntilIntakeCompletion,
      /// <summary>
      /// The <see cref="Phase.Transformation"/> phase will never start.
      /// Note that as a result, the task returned by the <see cref="IOrchestrator.ExecuteAsync()"/> method will never complete.
      /// This setting is for testing purposes only!
      /// </summary>
      Indefinitely
   }


   /// <summary>
   /// Timing when Data Conveyer will be allowed to start the <see cref="Phase.Output"/> phase.
   /// </summary>
   public enum DeferOutput
   {
      /// <summary>
      /// Data Conveyer will start the <see cref="Phase.Output"/> phase at the earliest timing allowed.
      /// This timing depends on several factors, for example <see cref="OrchestratorConfig.InputDataKind"/>, <see cref="OrchestratorConfig.OutputDataKind"/>
      /// or  <see cref="OrchestratorConfig.AllowTransformToAlterFields"/> settings.
      /// </summary>
      Auto,
      /// The <see cref="Phase.Output"/> phase will never start.
      /// Note that as a result, the task returned by the <see cref="IOrchestrator.ExecuteAsync()"/> method will never complete.
      /// This setting is for testing purposes only!
      Indefinitely
   }

   /// <summary>
   /// Indicates timing when Data Conveyer can start the Output process (ReadyForOutput method).
   /// </summary>
   internal enum OutputStartTiming
   {
      /// <summary>
      ///  At the very beginning of the process, i.e. before starting the pipeline (aka RFO#1).
      /// </summary>
      StartOfPipeline,
      /// <summary>
      /// After processing the header row, i.e. the first row on Intake (aka RFO#2).
      /// </summary>
      HeaderRowProcessed,
      /// <summary>
      /// After all data been processed through line parsing block (aka RFO#3).
      /// </summary>
      LineParsingCompleted,
      /// <summary>
      /// After all data has been processed through (all) transforming block(s) (aka RFO#4).
      /// </summary>
      TransformCompleted
   }


   /// <summary>
   /// Flags defining entities that the property bin objects are attached to.
   /// </summary>
   [Flags]
   public enum PropertyBinAttachedTo
   {
      /// <summary>
      /// No property bins in use.
      /// </summary>
      Nothing = 0,
      /// <summary>
      /// Property bin attached to every record.
      /// </summary>
      Records = 1,
      /// <summary>
      /// Property bin attached to every cluster.
      /// </summary>
      Clusters = 2
   }
}
