//FileLogger.cs
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


using System.IO;

namespace Mavidian.DataConveyer.Logging
{
   /// <summary>
   /// A simple logger that sends log entries to a text file.
   /// It operates in append mode (prior contents of the log file is preserved).
   /// It needs exclusive access to the file, so no concurrent access is allowed.
   /// </summary>
   public sealed class FileLogger : ILogger
   {
      private readonly StreamWriter _writer;

      /// <summary>
      /// Type of the logger, i.e. <see cref="LoggerType.LogFile"/>.
      /// </summary>
      public LoggerType LoggerType { get => LoggerType.LogFile; }

      /// <summary>
      /// Description of the process being logged; placed in the logger header.
      /// May be helpful in distinguishing between differenet entries place in the same log.
      /// </summary>
      public string LogDescription { get; }

      /// <summary>
      /// The least severe severity level to be logged.
      /// If not assigned during logger construction, a level of <see cref="LogEntrySeverity.None"/> will be assumed (i.e. no entries will be logged).
      /// </summary>
      public LogEntrySeverity LoggingThreshold { get; }

      internal FileLogger(LogEntrySeverity threshold, string description, StreamWriter writer)
      {
         writer.AutoFlush = true;
         _writer = writer;
         LoggingThreshold = threshold;
         LogDescription = description;
      }


      /// <summary>
      /// Send the log entry to a text file (log file).
      /// </summary>
      /// <param name="entry">Log entry to send.</param>
      public void Log(LogEntry entry)
      {
         if (entry.Severity <= LoggingThreshold)  //lower value means more severe(!)
         {
            string prefix; //FATAL, ERROR, WARN, INFO or DEBUG
            switch (entry.Severity)
            {
               case LogEntrySeverity.None: prefix = string.Empty; break;
               case LogEntrySeverity.Fatal: prefix = "FATAL "; break;
               case LogEntrySeverity.Error: prefix = "ERROR "; break;
               case LogEntrySeverity.Warning: prefix = "WARN  "; break;
               case LogEntrySeverity.Information: prefix = "INFO  "; break;
               case LogEntrySeverity.Debug: prefix = "DEBUG "; break;
               default: prefix = "      "; break;
            }

            _writer.WriteLine(prefix + entry.MessageOnDemand());

            //Note that this WriteLine method may throw ObjectDisposedException: Cannot write to a closed TextWriter
            // in case Logger was disposed (which happens by default when orchestrator is disposed).
            // To make Data Conveyer behavior more explicit (and prevent log messages being "swallowed"), such exception
            // is not caught, but instead CloseLoggerOnDispose config setting (true by default) can be set to false,
            // in which case log messages can continue to be written to the log file after orchestrator's disposal.
            //Also note that if CloseLoggerOnDispose was set to false, then any new orchestrator may need to reuse
            // the logger instance.
         }
      }

      /// <summary>
      /// Dispose the underlying text writer.
      /// </summary>
      public void Dispose()
      {
         //_writer.Flush();
         //_writer.Close();
         _writer.Dispose();  //it also does Flush/Close
      }
   }
}
