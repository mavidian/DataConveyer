//LoggerHelpers.cs
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


using Mavidian.DataConveyer.Common;
using System;
using System.Text;

namespace Mavidian.DataConveyer.Logging
{
   /// <summary>
   /// Extension methods to ILogger interface to post log entries in various scenarios
   /// </summary>
   internal static class LoggerHelpers
   {
      //Note that 2nd parameter of LogEntry ctor (messageOnDemand) is evaluated lazily, which improves performance in cases where complex
      // log entries are likely to be discarded by the logger (e.g. NullLogger or below severity threshold).
      //This is particularly important for low severity messages (such as long Debug messages), which are not only more complex to evaluate,
      // but also less likely to be logged.

#region Log Debug (verbose) entries

      /// <summary>
      /// Log simple debug (verbose) message
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="message">Message to report</param>
      internal static void LogDebug(this ILogger logger, string message)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Debug, () => Embellish(timeStamp, message)));
      }

      /// <summary>
      /// Log debug (verbose) message on demand
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="messageOnDemand">Function returning message to report</param>
      internal static void LogDebug(this ILogger logger, Func<string> messageOnDemand)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Debug, () => Embellish(timeStamp, messageOnDemand())));
      }

#endregion Log Debug (verbose) entries


#region Log Informational entries

      /// <summary>
      /// Log simple informational message
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="message">Message to report</param>
      internal static void LogInfo(this ILogger logger, string message)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Information, () => Embellish(timeStamp, message)));
      }

      /// <summary>
      /// Log informational message on demand
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="messageOnDemand">Function returning message to report</param>
      internal static void LogInfo(this ILogger logger, Func<string> messageOnDemand)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Information, () => Embellish(timeStamp, messageOnDemand())));
      }

#endregion Log Informational entries


#region Log Warning entries

      /// <summary>
      /// Log simple warning message
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="message">Message to report</param>
      internal static void LogWarning(this ILogger logger, string message)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Warning, () => Embellish(timeStamp, message)));
      }

      /// <summary>
      /// Log warning message on demand
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="messageOnDemand">Function returning message to report</param>
      internal static void LogWarning(this ILogger logger, Func<string> messageOnDemand)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Warning, () => Embellish(timeStamp, messageOnDemand())));
      }

#endregion Log Warning entries


#region Log Error entries

      /// <summary>
      /// Log error message for a given exception
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="exception">Exception to report</param>
      internal static void LogError(this ILogger logger, Exception exception)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Error, () => Embellish(timeStamp, $"Exception of type { exception.GetType() } occurred: {exception.Message}"), exception));
      }

      /// <summary>
      /// Log error message for a given message and exception
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="message">Message to report</param>
      /// <param name="exception">Exception to report</param>
      internal static void LogError(this ILogger logger, string message, Exception exception)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Error, () => Embellish(timeStamp, $"{message}\r\n-->Exception of type { exception.GetType() } occurred: {exception.Message}"), exception));
      }

      /// <summary>
      /// Log simple error message
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="message">Message to report</param>
      internal static void LogError(this ILogger logger, string message)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Error, () => Embellish(timeStamp, message)));
      }

      /// <summary>
      /// Log error message on demand
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="messageOnDemand">Function returning message to report</param>
      internal static void LogError(this ILogger logger, Func<string> messageOnDemand)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Error, () => Embellish(timeStamp, messageOnDemand())));
      }

#endregion Log Error entries


#region Log Fatal entries

      /// <summary>
      /// Log fatal error message for a given exception
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="exception">Exception to report</param>
      internal static void LogFatal(this ILogger logger, Exception exception)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Fatal, () => Embellish(timeStamp, $"Exception of type { exception.GetType() } occurred: {exception.Message}"), exception));
      }

      /// <summary>
      /// Log fatal error message for a given message and exception
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="message">Message to report</param>
      /// <param name="exception">Exception to report</param>
      internal static void LogFatal(this ILogger logger, string message, Exception exception)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Fatal, () => Embellish(timeStamp, $"{message}\r\n-->Exception of type { exception.GetType() } occurred: {exception.Message}"), exception));
      }

      /// <summary>
      /// Log simple fatal error message
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="message">Message to report</param>
      internal static void LogFatal(this ILogger logger, string message)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.Fatal, () => Embellish(timeStamp, message)));
      }

#endregion Log Fatal entries


      /// <summary>
      /// Insert the Title Box (header) to the log
      /// </summary>
      /// <param name="logger"></param>
      /// <param name="configName"></param>
      internal static void LogStart(this ILogger logger, string configName)
      {
         var line = new string('#', 70);
         var title = new StringBuilder(Environment.NewLine);
         title.AppendLine(line);
         title.Append("# Data Conveyer v");
         title.AppendLine(ProductInfo.CurrentInfo.FileVersion);
         title.Append(string.IsNullOrEmpty(configName) ? "# Unnamed configuration" : $"# Config name: {configName}");
         title.Append("; Logging threshold: ");
         title.AppendLine(logger.LoggingThreshold.ToString());
         title.Append("# Description: ");
         title.AppendLine(logger.LogDescription);
         title.Append("# Logging started on ");
         title.AppendLine(DateTime.Now.ToString("MM-dd-yyyy HH:mm:ss.fff"));
         title.Append(line);
         logger.Log(new LogEntry(LogEntrySeverity.None, () => title.ToString()));
      }


      /// <summary>
      /// Insert the logging footer message
      /// </summary>
      /// <param name="logger"></param>
      internal static void LogEnd(this ILogger logger)
      {
         var timeStamp = DateTime.Now;  //so that log entry contains time message was posted by DataConveyr (not when logger placed it on the log)
         logger.Log(new LogEntry(LogEntrySeverity.None, () => $"### Logging closed on {timeStamp.ToString("MM-dd-yyyy HH:mm:ss.fff")} ###"));
      }


      //TODO: Additional extension methods that (lazily) construct log entries in other scenarios
      //      As number of these methods grow, consider splitting the LoggerHelpers class (LoggerHelpers_Errors or LoggerHelpers_KeyValRecord)


      /// <summary>
      /// Helper function to precede a message by a standard leader consisting of date/time and a DataConveyer version
      /// </summary>
      /// <param name="timeStamp"></param>
      /// <param name="message"></param>
      /// <returns></returns>
      private static string Embellish(DateTime timeStamp, string message)
      {
         return $"{ timeStamp.ToString("MM-dd-yyyy HH:mm:ss.fff")}: { message}";
      }

   }
}
