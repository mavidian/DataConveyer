//Orchestrator_tests.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Logging;
using Mavidian.DataConveyer.Orchestrators;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.IO;
using System.Reflection;
using System.Threading;

namespace DataConveyer_tests.Orchestrators
{
   [TestClass]
   public class OrchestratorConfig_tests
   {
      //These tests use mock configuration

      [TestMethod]
      public void Ctor_NoExplicitSettings_DefaultConfig()
      {
         //arrange
         var config = new OrchestratorConfig();

         //act

         //assert
         //Logger:
         config.Logger.LoggerType.Should().Be(LoggerType.Null);
         config.Logger.LoggingThreshold.Should().Be(LogEntrySeverity.None);
         // Environmental settings:
         config.ConfigName.Should().BeNull();
         config.DataConveyerInfo.Should().BeOfType(typeof(ProductInfo));
         config.DataConveyerInfo.Name.Should().Be("DataConveyer");
         var ver = Assembly.GetAssembly(typeof(EtlOrchestrator)).GetName().Version;
         config.DataConveyerInfo.Version.Should().Be($"{ver.Major}.{ver.Minor}");
         config.DataConveyerInfo.Target.Should().Be("Net45");  // Tests project targets net45 and the referenced library matches it
         //General settings:
         config.ExplicitTypeDefinitions.Should().BeNull();
         config.TypeDefiner.Should().BeOfType(typeof(Func<string, ItemDef>));
         config.TypeDefiner.Should().Be(OrchestratorConfig.DefaultTypeDefiner);
         config.TypeDefiner("anyKey").Type.Should().Be(ItemType.String);
         config.EagerInitialization.Should().BeFalse();
         config.BufferSize.Should().Be(-1);
         config.IntakeRecordLimit.Should().Be(Constants.Unlimited);
         config.TimeLimit.Should().Be(Timeout.InfiniteTimeSpan);
         config.ReportProgress.Should().BeFalse();
         config.ProgressInterval.Should().Be(0);
         config.PhaseStartingHandler.Should().BeNull();
         config.ProgressChangedHandler.Should().BeNull();
         config.PhaseFinishedHandler.Should().BeNull();
         config.ErrorOccurredHandler.Should().BeNull();
         //Intake specific settings:
         config.InputDataKind.Should().Be(KindOfTextData.Raw);
         config.InputFileNames.Should().BeNull();
         config.AsyncIntake.Should().BeFalse();
         config.IntakeSupplier.Should().Be(OrchestratorConfig.DefaultIntakeSupplier);
         config.AsyncIntakeSupplier.Should().Be(OrchestratorConfig.DefaultAsyncIntakeSupplier);
         config.IntakeInitializer.Should().Be(OrchestratorConfig.DefaultIntakeInitializer);
         config.IntakeDisposer.Should().Be(OrchestratorConfig.DefaultIntakeDisposer);
         config.IntakeBufferFactor.Should().Be(1.5);
         config.HeadersInFirstInputRow.Should().BeFalse();
         config.InputHeadersRepeated.Should().BeTrue();
         config.RetainQuotes.Should().BeFalse();
         config.TrimInputValues.Should().BeFalse();
         config.InputKeyPrefix.Should().BeNull();
         config.ExcludeItemsMissingPrefix.Should().BeFalse();
         config.ActionOnDuplicateKey.Should().Be(ActionOnDuplicateKey.IgnoreItem);
         config.ClusterMarker.Should().Be(OrchestratorConfig.DefaultClusterMarker);
         config.MarkerStartsCluster.Should().BeTrue();
         config.InputFields.Should().BeNull();
         config.ArbitraryInputDefs.Should().BeNull();
         config.DefaultInputFieldWidth.Should().Be(10);
         config.AllowOnTheFlyInputFields.Should().BeFalse();
         //Transformer specific settings:
         config.TransformerType.Should().Be(TransformerType.Recordbound);
         config.ClusterboundTransformer.Should().Be(OrchestratorConfig.DefaultClusterboundTransformer);
         config.RecordboundTransformer.Should().Be(OrchestratorConfig.DefaultRecordboundTransformer);
         config.ClusterFilterPredicate.Should().Be(OrchestratorConfig.DefaultClusterFilterPredicate);
         config.RecordFilterPredicate.Should().Be(OrchestratorConfig.DefaultRecordFilterPredicate);
         config.UniversalTransformer.Should().Be(OrchestratorConfig.DefaultUniversalTransformer);
         config.ConcurrencyLevel.Should().Be(1);
         config.TransformBufferFactor.Should().Be(1);
         config.AllowTransformToAlterFields.Should().BeFalse();
         //Output specific settings:
         config.RouterType.Should().Be(RouterType.SingleTarget);
         config.ClusterRouter.Should().Be(OrchestratorConfig.DefaultClusterRouter);
         config.RecordRouter.Should().Be(OrchestratorConfig.DefaultRecordRouter);
         config.OutputDataKind.Should().Be(KindOfTextData.Raw);
         config.OutputFileNames.Should().BeNull();
         config.AsyncOutput.Should().BeFalse();
         config.OutputConsumer.Should().Be(OrchestratorConfig.DefaultOutputConsumer);
         config.AsyncOutputConsumer.Should().Be(OrchestratorConfig.DefaultAsyncOutputConsumer);
         config.OutputInitializer.Should().Be(OrchestratorConfig.DefaultOutputInitializer);
         config.OutputDisposer.Should().Be(OrchestratorConfig.DefaultOutputDisposer);
         config.OutputBufferFactor.Should().Be(1.5);
         config.HeadersInFirstOutputRow.Should().BeFalse();
         config.RepeatOutputHeaders.Should().BeTrue();
         config.QuotationMode.Should().Be(QuotationMode.OnlyIfNeeded);
         config.TrimOutputValues.Should().BeFalse();
         config.OutputKeyPrefix.Should().BeNull();
         config.OutputFields.Should().BeNull();
         config.ArbitraryOutputDefs.Should().BeNull();
         config.DefaultOutputFieldWidth.Should().Be(10);
         config.LeaderContents.Should().BeNull();
         config.RepeatLeaders.Should().BeTrue();
         config.TrailerContents.Should().BeNull();
         config.RepeatTrailers.Should().BeTrue();
         config.ExcludeExtraneousFields.Should().BeFalse();
      }


      [TestMethod]
      public void Ctor_SomeExplicitSettings_SettingsAccepted()
      {
         //arrange
         var config = new OrchestratorConfig(LoggerCreator.CreateLogger(LoggerType.LogFile, "Test", LogEntrySeverity.Error));

         //act
         // note that this is not a sensible set of settings (!)
         config.ExplicitTypeDefinitions = "DOB|D,Amount|M,Name|S";
         config.TypeDefiner = fn => new ItemDef(ItemType.Int, "000");  //everything int, except for 3 exceptions above
         config.MarkerStartsCluster = false;
         config.BufferSize = 7;
         config.InputDataKind = KindOfTextData.Arbitrary;
         config.AsyncIntake = true;
         config.RetainQuotes = true;
         config.ActionOnDuplicateKey = ActionOnDuplicateKey.AssignDefaultKey;
         config.DefaultInputFieldWidth = 6;
         config.TransformerType = TransformerType.ClusterFilter;
         config.ClusterFilterPredicate = c => false;
         config.RecordFilterPredicate = r => false;  //makes no sense in the orchestrator, but good to test
         config.ConcurrencyLevel = 19;
         config.OutputDataKind = KindOfTextData.Flat;
         config.OutputFileNames = "blahblah";
         config.QuotationMode = QuotationMode.StringsAndDates;
         config.TrimOutputValues = true;
         config.OutputKeyPrefix = "pfx_";

         //assert
         //Logger:
         config.Logger.LoggerType.Should().Be(LoggerType.LogFile);
         config.Logger.LoggingThreshold.Should().Be(LogEntrySeverity.Error);
         // Environmental settings:
         config.ConfigName.Should().BeNull();
         config.DataConveyerInfo.Should().BeOfType(typeof(ProductInfo));
         config.DataConveyerInfo.Name.Should().Be("DataConveyer");
         var ver = Assembly.GetAssembly(typeof(EtlOrchestrator)).GetName().Version;
         config.DataConveyerInfo.Version.Should().Be($"{ver.Major}.{ver.Minor}");
         //General settings:
         config.ExplicitTypeDefinitions.Should().Be("DOB|D,Amount|M,Name|S");
         config.TypeDefiner.Should().BeOfType(typeof(Func<string, ItemDef>));
         config.TypeDefiner.Should().NotBe(OrchestratorConfig.DefaultTypeDefiner);
         config.TypeDefiner("anyKey").Type.Should().Be(ItemType.Int);
         config.EagerInitialization.Should().BeFalse();
         config.BufferSize.Should().Be(7);
         config.ReportProgress.Should().BeFalse();
         config.ProgressInterval.Should().Be(0);
         config.PhaseStartingHandler.Should().BeNull();
         config.ProgressChangedHandler.Should().BeNull();
         config.PhaseFinishedHandler.Should().BeNull();
         //Intake specific settings:
         config.InputDataKind.Should().Be(KindOfTextData.Arbitrary);
         config.InputFileNames.Should().BeNull();
         config.AsyncIntake.Should().BeTrue();
         config.IntakeSupplier.Should().Be(OrchestratorConfig.DefaultIntakeSupplier);
         config.AsyncIntakeSupplier.Should().Be(OrchestratorConfig.DefaultAsyncIntakeSupplier);
         config.IntakeBufferFactor.Should().Be(1.5);
         config.HeadersInFirstInputRow.Should().BeFalse();
         config.RetainQuotes.Should().BeTrue();
         config.TrimInputValues.Should().BeFalse();
         config.InputKeyPrefix.Should().BeNull();
         config.ExcludeItemsMissingPrefix.Should().BeFalse();
         config.ActionOnDuplicateKey.Should().Be(ActionOnDuplicateKey.AssignDefaultKey);
         config.ClusterMarker.Should().Be(OrchestratorConfig.DefaultClusterMarker);
         config.MarkerStartsCluster.Should().BeFalse();
         config.InputFields.Should().BeNull();
         config.ArbitraryInputDefs.Should().BeNull();
         config.DefaultInputFieldWidth.Should().Be(6);
         config.AllowOnTheFlyInputFields.Should().BeFalse();
         //Transformer specific settings:
         config.TransformerType.Should().Be(TransformerType.ClusterFilter);
         config.ClusterboundTransformer.Should().Be(OrchestratorConfig.DefaultClusterboundTransformer);
         config.RecordboundTransformer.Should().Be(OrchestratorConfig.DefaultRecordboundTransformer);
         config.ClusterFilterPredicate.Should().BeOfType(typeof(Func<ICluster, bool>));
         config.ClusterFilterPredicate.Should().NotBe(OrchestratorConfig.DefaultClusterFilterPredicate);
         config.ClusterFilterPredicate(null).Should().BeFalse();
         config.RecordFilterPredicate.Should().NotBe(OrchestratorConfig.DefaultRecordFilterPredicate);
         config.RecordFilterPredicate(null).Should().BeFalse();    // this predicate would never be called (due to TransformerType setting)
         config.UniversalTransformer.Should().Be(OrchestratorConfig.DefaultUniversalTransformer);
         config.ConcurrencyLevel.Should().Be(19);
         config.TransformBufferFactor.Should().Be(1);
         config.AllowTransformToAlterFields.Should().BeFalse();
         //Output specific settings:
         config.OutputDataKind.Should().Be(KindOfTextData.Flat);
         config.OutputFileNames.Should().Be("blahblah");
         config.AsyncOutput.Should().BeFalse();
         config.OutputConsumer.Should().Be(OrchestratorConfig.DefaultOutputConsumer);
         config.AsyncOutputConsumer.Should().Be(OrchestratorConfig.DefaultAsyncOutputConsumer);
         config.OutputBufferFactor.Should().Be(1.5);
         config.HeadersInFirstOutputRow.Should().BeFalse();
         config.QuotationMode.Should().Be(QuotationMode.StringsAndDates);
         config.TrimOutputValues.Should().BeTrue();
         config.OutputKeyPrefix.Should().Be("pfx_");
         config.OutputFields.Should().BeNull();
         config.ArbitraryOutputDefs.Should().BeNull();
         config.DefaultOutputFieldWidth.Should().Be(10);
         config.LeaderContents.Should().BeNull();
         config.TrailerContents.Should().BeNull();
         config.ExcludeExtraneousFields.Should().BeFalse();
      }


      [TestMethod]
      public void DataConveyerVersion_any_CorrectData()
      {
         //arrange
         var config = new OrchestratorConfig();
         var currAssembly = Assembly.GetAssembly(typeof(EtlOrchestrator));

         //act

         //assert
         config.DataConveyerInfo.Should().BeOfType(typeof(ProductInfo));
         config.DataConveyerInfo.Name.Should().Be("DataConveyer");
         var ver = Assembly.GetAssembly(typeof(EtlOrchestrator)).GetName().Version;
         config.DataConveyerInfo.Version.Should().Be($"{ver.Major}.{ver.Minor}");
         config.DataConveyerInfo.Copyright.Should().Be("Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.");
         config.DataConveyerInfo.Date.Should().Be(File.GetLastWriteTime(currAssembly.Location));
      }

   }
}
