//Logger_tests_KeyValRecordEntries.cs
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
using System.Collections.Concurrent;

namespace DataConveyer_tests.Logging
{
   [TestClass]
   public class Logger_tests_KeyValRecordEntries
   {
      //This class contains integration tests

      private OrchestratorConfig _config;
      private TypeDefinitions _typeDefs;

      [TestInitialize()]
      public void Initialize()
      {
         _config = new OrchestratorConfig(new MockLogger(LogEntrySeverity.Debug)); //log all levels

         // simple type definitions, everything string
         _typeDefs = new TypeDefinitions(k => ItemType.String, new ConcurrentDictionary<string, ItemType>(), k => string.Empty, new ConcurrentDictionary<string, string>());
      }


      [TestMethod]
      public void ConstructRecordWithDupKey_OutsideOfThreshold_NothingLogged()
      {
         //arrange
         var config = new OrchestratorConfig(new MockLogger(LogEntrySeverity.Error));  //Warning (sent as a result of dup) is outside of this threshold

         var items = new IItem[] { KeyValItem.CreateItem("Fld1", "71941", _typeDefs),
                                   KeyValItem.CreateItem("Fld2", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("Fld1", 243, _typeDefs) };  //this field has the same key as the first field
         int recNo = 25;

         //act
         var logger = (MockLogger)config.Logger;
         var rec = new KeyValRecord(items, recNo, 1, 0, null, null, null, _typeDefs, config, null, ActionOnDuplicateKey.IgnoreItem);

         //assert
         logger.Results.Count.Should().Be(1);  //Only Log Title
         logger.Results[0].Item1.Should().Be(LogEntrySeverity.None);
      }


      [TestMethod]
      public void ConstructRecordWithDupKey_WithinThresholdIgnoreDups_WarningLogged()
      {
         //This is really an integration test

         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("Fld1", "71941", _typeDefs),
                                   KeyValItem.CreateItem("Fld2", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("Fld1", 243, _typeDefs) };  //this field has the same key as the first field
         int recNo = 25;

         //act
         var logger = (MockLogger)_config.Logger;
         var rec = new KeyValRecord(items, recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.IgnoreItem);

         //assert
         //   Item1=severity, Item2=message (evaluated by logger), Item3=entire log entry (passed through by logger)
         logger.Results.Count.Should().Be(3);  //incl. Log Title Box and "Configuration initialized." entry
         var result = logger.Results[2];
         result.Item1.Should().Be(LogEntrySeverity.Warning);
         result.Item2.Contains("Duplicate key in field #3 of record #25:").Should().BeTrue();  //text from KeyValRecord class
         result.Item2.Contains("Item containing value of '243' has been ignored.").Should().BeTrue();
         var entry = result.Item3;
         result.Item2.Should().Be(entry.MessageOnDemand());  //this confirms that timestamp in a message is from a time of LogInfo method
         entry.Severity.Should().Be(LogEntrySeverity.Warning);
         entry.MessageOnDemand.Should().BeOfType(typeof(Func<string>));
         entry.Exception.Should().BeNull();
         var msg = result.Item3.MessageOnDemand(); //evaluate message on demand one more time
         msg.Contains("Duplicate key in field #3 of record #25:").Should().BeTrue();
         msg.Contains("Item containing value of '243' has been ignored.").Should().BeTrue();
         msg.Should().Be(entry.MessageOnDemand());  //this confirms again that timestamp in a message is from a time of LogInfo method
      }


      [TestMethod]
      public void ConstructRecordWithDupKey_WithinThresholdExcludeIfDup_ErrorLogged()
      {
         //This is really an integration test

         //arrange
         var items = new IItem[] { KeyValItem.CreateItem("Fld1", "71941", _typeDefs),
                                   KeyValItem.CreateItem("Fld2", "blahblah", _typeDefs),
                                   KeyValItem.CreateItem("Fld1", 243, _typeDefs) };  //this field has the same key as the first field
         int recNo = 25;

         //act
         var logger = (MockLogger)_config.Logger;
         var rec = new KeyValRecord(items, recNo, 1, 0, null, null, null, _typeDefs, _config, null, ActionOnDuplicateKey.ExcludeRecord);

         //assert
         //   Item1=severity, Item2=message (evaluated by logger), Item3=entire log entry (passed through by logger)
         logger.Results.Count.Should().Be(3);  //incl. Log Title Box and "Configuration initialized." entry
         var result = logger.Results[2];
         result.Item1.Should().Be(LogEntrySeverity.Error);
         result.Item2.Contains("Duplicate key in field #3 of record #25:").Should().BeTrue();  //text from KeyValRecord class
         result.Item2.Contains("Record has been excluded from processing.").Should().BeTrue();
         var entry = result.Item3;
         result.Item2.Should().Be(entry.MessageOnDemand());  //this confirms that timestamp in a message is from a time of LogInfo method
         entry.Severity.Should().Be(LogEntrySeverity.Error);
         entry.MessageOnDemand.Should().BeOfType(typeof(Func<string>));
         entry.Exception.Should().BeNull();
         var msg = result.Item3.MessageOnDemand(); //evaluate message on demand one more time
         msg.Contains("Duplicate key in field #3 of record #25:").Should().BeTrue();
         msg.Contains("Record has been excluded from processing.").Should().BeTrue();
         msg.Should().Be(entry.MessageOnDemand());  //this confirms again that timestamp in a message is from a time of LogInfo method
      }
   }
}
