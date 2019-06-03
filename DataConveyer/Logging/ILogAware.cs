//ILogAware.cs
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
//  - interface ILogAware
//  - class LogAwareHelpers

using System;

namespace Mavidian.DataConveyer.Logging
{
   //The idea was adopted from https://coderwall.com/p/hrm7fw/use-c-magic-to-turn-any-class-into-log-aware-class

   /// <summary>
   /// An empty interface to facilitate access to the logger by any class that implements it.
   /// </summary>
   internal interface ILogAware { }

   /// <summary>
   /// Helper class that extends the ILogAware interface.
   /// It contains extension methods that are available to any class that implements the ILogAware interface.
   /// The purpose of these methods is to send log messages to current logger; they mirror the methods located 
   /// in the <see cref="LoggerHelpers"/> class that extend the <see cref="ILogger"/> interface.
   /// </summary>
   internal static class LogAwareHelpers
   {
      internal static void Log(this ILogAware subject, LogEntry logEntry)
      {
         ExecuteLogAction(l => l.Log(logEntry));
         //The above line is equivalent to:
         //  var logger = LoggerCreator.CurrentLogger;
         //  logger?.Log(logEntry);

      }

      internal static void LogDebug(this ILogAware subject, string message) { ExecuteLogAction(l => l.LogDebug(message)); }
      internal static void LogDebug(this ILogAware subject, Func<string> messageOnDemand) { ExecuteLogAction(l => l.LogDebug(messageOnDemand)); }
      internal static void LogInfo(this ILogAware subject, string message) { ExecuteLogAction(l => l.LogInfo(message)); }
      internal static void LogInfo(this ILogAware subject, Func<string> messageOnDemand) { ExecuteLogAction(l => l.LogInfo(messageOnDemand)); }
      internal static void LogWarning(this ILogAware subject, string message) { ExecuteLogAction(l => l.LogWarning(message)); }
      internal static void LogWarning(this ILogAware subject, Func<string> messageOnDemand) { ExecuteLogAction(l => l.LogWarning(messageOnDemand)); }
      internal static void LogError(this ILogAware subject, Exception exception) { ExecuteLogAction(l => l.LogError(exception)); }
      internal static void LogError(this ILogAware subject, string message, Exception exception) { ExecuteLogAction(l => l.LogError(message, exception)); }
      internal static void LogError(this ILogAware subject, string message) { ExecuteLogAction(l => l.LogError(message)); }
      internal static void LogError(this ILogAware subject, Func<string> messageOnDemand) { ExecuteLogAction(l => l.LogError(messageOnDemand)); }
      internal static void LogFatal(this ILogAware subject, Exception exception) { ExecuteLogAction(l => l.LogFatal(exception)); }
      internal static void LogFatal(this ILogAware subject, string message, Exception exception) { ExecuteLogAction(l => l.LogFatal(message, exception)); }
      internal static void LogFatal(this ILogAware subject, string message) { ExecuteLogAction(l => l.LogFatal(message)); }

      //Note that methods, such as LogStart or LogEnd could be added here as well, but they're not really needed to be called via ILogAware interface

      /// <summary>
      /// Helper method common to all other extension methods. It get a hold of the logger
      /// and executes the logging action passed to it (by calling a corresponding extension
      /// method of the ILogger interface).
      /// </summary>
      /// <param name="logAction">Logging action to perform.</param>
      private static void ExecuteLogAction(Action<ILogger> logAction)
      {
         var logger = LoggerCreator.CurrentLogger;
         if (logger != null) logAction(logger);
      }
   }
}
