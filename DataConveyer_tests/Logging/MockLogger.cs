//MockLogger.cs
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


using Mavidian.DataConveyer.Logging;
using System;
using System.Collections.Generic;

namespace DataConveyer_tests.Logging
{
   /// <summary>
   /// This logger is for testing purposes only!
   /// It evaluates log entry severity and if meeting threshold criteria adds it
   ///  to Result list, which is available for inspection by the tester. 
   /// </summary>
   internal class MockLogger : ILogger
   {
      internal List<Tuple<LogEntrySeverity, string, LogEntry>> Results { get; private set; }  //Item1=severity, Item2=message, Item3=entire entry sent to logger
      public LoggerType LoggerType => LoggerType.Null;  //it doesn't really matter
      public string LogDescription => string.Empty;
      public LogEntrySeverity LoggingThreshold { get ; set; }
      private LogEntrySeverity _threshold;

      internal MockLogger(LogEntrySeverity threshold)
      {
         _threshold = threshold;
         Results = new List<Tuple<LogEntrySeverity, string, LogEntry>>();
      }

      public void Log(LogEntry entry)
      {
         if (entry.Severity <= _threshold)  //lower value means more severe(!)
         {
            Results.Add(Tuple.Create(entry.Severity, entry.MessageOnDemand(), entry));
         }
      }


      public void Dispose() { }

      /// <summary>
      /// Log simple informational message, but throw Exception upon attempt to evaluate messageOnDemand (FOR TESTING ONLY!!!)
      /// </summary>
      /// <param name="message">Message to report</param>
      internal void LogInfoThrowUponEval(string message)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time when message was posted by DataConveyr (not when logger placed it on the log)
         Log(new LogEntry(LogEntrySeverity.Information, () => { throw new InvalidOperationException("no-message-this-time"); }));
      }

   }
}
