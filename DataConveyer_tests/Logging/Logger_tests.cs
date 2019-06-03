//Logger_tests.cs
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
using Mavidian.DataConveyer.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;

namespace DataConveyer_tests.Logging
{
   [TestClass]
   public class Logger_tests
   {

      [TestMethod]
      public void LogInfoSimple_NullLogger_NoMessageOnDemandCalled()
      {
         //arrange
         ILogger logger = new NullLogger();

         //act
         Action slowLog = () => logger.LogInfo("dummy message");

         //assert
         slowLog.Should().NotThrow();
      }


      [TestMethod]
      public void LogInfoSimple_ConsumingLogger_MessageOnDemandCalled()
      {
         //ConsumingLogger means that when Log method called, it consumes the MessageOnDemand (MockLogger does that)

         //arrange
         var logger = new MockLogger(LogEntrySeverity.Debug);

         //act
         Action slowLog = () => logger.LogInfoThrowUponEval("dummy message");

         //assert
         slowLog.Should().Throw<InvalidOperationException>().WithMessage("no-message-this-time");
      }


      [TestMethod]
      public void LogInfoSimple_ConsumingLogger_LogEntryLogged()
      {
         //ConsumingLogger means that when Log method called, it consumes the MessageOnDemand (MockLogger does that)

         //arrange
         var logger = new MockLogger(LogEntrySeverity.Debug);

         //act
         logger.LogInfo("dummy message");

         //assert
         //   Item1=severity, Item2=message (evaluated by logger), Item3=entire log entry (passed through by logger)
         logger.Results.Count.Should().Be(1);
         var result = logger.Results[0];
         result.Item1.Should().Be(LogEntrySeverity.Information);
         result.Item2.Contains("dummy message").Should().BeTrue();
         var entry = result.Item3;
         result.Item2.Should().Be(entry.MessageOnDemand());  //this confirms that timestamp in a message is from a time of LogInfo method
         entry.Severity.Should().Be(LogEntrySeverity.Information);
         entry.MessageOnDemand.Should().BeOfType(typeof(Func<string>));
         entry.Exception.Should().BeNull();
         var msg = result.Item3.MessageOnDemand(); //evaluate message on demand one more time
         msg.Contains("dummy message").Should().BeTrue();
         msg.Should().Be(entry.MessageOnDemand());  //this confirms again that timestamp in a message is from a time of LogInfo method
      }


      [TestMethod]
      public void LogOnDemand_WithinThreshold_OnDemandMessageConsumed()
      {
         //arrange
         var logger = new MockLogger(LogEntrySeverity.Debug);

         //act
         Action logOnDemand = () => { logger.LogInfo(() => { throw new InvalidOperationException("ha-ha-ha"); }); };

         //assert
         logOnDemand.Should().Throw<InvalidOperationException>().WithMessage("ha-ha-ha");
      }


      [TestMethod]
      public void LogOnDemand_OutsideOfThreshold_OnDemandMessageNotConsumed()
      {
         //arrange
         var logger = new MockLogger(LogEntrySeverity.None);  //no messages on demand consumed

         //act
         Action logOnDemand = () => { logger.LogInfo(() => { throw new InvalidOperationException("ha-ha-ha"); }); };

         //assert
         logOnDemand.Should().NotThrow();
      }

   }
}
