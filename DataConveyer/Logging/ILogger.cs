//ILogger.cs
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
//  - interface ILogger
//  - enum LoggerType
//  - class NullLogger

using System;

namespace Mavidian.DataConveyer.Logging
{
   /// <summary>
   /// Logging facade to be implemented by the actual logger, e.g. NLog.
   /// </summary>
   public interface ILogger : IDisposable
   {
      /// <summary>
      /// Type of the logger.
      /// In case of problems creating the logger of given type, <see cref="LoggerType.Null"/> logger is assumed.
      /// </summary>
      LoggerType LoggerType { get; }

      /// <summary>
      /// Description of the process being logged; placed in the logger header.
      /// May be helpful in distinguishing between differenet entries place in the same log.
      /// </summary>
      string LogDescription { get; }

      /// <summary>
      /// The least severe severity level of log entries to be logged.  One of:
      /// <list type="bullet">
      ///  <item><term>None</term><description>No log entries will be logged at all (default)</description></item>
      ///  <item><term>Fatal</term><description>Only fatal errors, i.e. those that caused process to halt will be logged</description></item>
      ///  <item><term>Error</term><description>All errors, either fatal or those that allowed process continuation (upon skipping some data) will be logged</description></item>
      ///  <item><term>Warning</term><description>In addition to errors, all warnings (scenarios where Data Conveyer was able to repair unexpected data) will be logged</description></item>
      ///  <item><term>Information</term><description>All log entries: errors, warnings and informational messages will be logged (except verbose debug messages intended for troubleshooting purposes)</description></item>
      ///  <item><term>Debug</term><description>All possible log entries will be logged</description></item>
      /// </list>
      /// If not specified during logger construction, a level of <see cref="LogEntrySeverity.None"/> will be assumed (i.e. no entries will be logged).
      /// See also <see cref="LogEntrySeverity"/>.
      /// </summary>
      LogEntrySeverity LoggingThreshold { get; }

      /// <summary>
      /// Log a given log entry.
      /// </summary>
      /// <param name="entry">Log entry to process (the entry.MessageOnDemand() will evaluate the actual message to report).</param>
      void Log(LogEntry entry);
   }


   /// <summary>
   /// Type of the logger to process the log entries
   /// </summary>
   public enum LoggerType
   {
      /// <summary>
      /// Logger that actually disregards log entries (default)
      /// </summary>
      Null = 1,
      /// <summary>
      /// Logger that sends (appends) entries as text to a log file
      /// </summary>
      LogFile = 2,
      /// <summary>
      /// Logger using the NLog logging framework (future use)
      /// </summary>
      NLog = 3
   }


   /// <summary>
   /// ILogger implementation that ignores all log entries sent to it
   /// </summary>
   internal class NullLogger : ILogger
   {
      public LoggerType LoggerType => LoggerType.Null;
      public string LogDescription => string.Empty;
      public LogEntrySeverity LoggingThreshold => LogEntrySeverity.None;
      public void Log(LogEntry entry) { }
      public void Dispose() { }
   }
}
