//LoggerCreator.cs
//
// Copyright © 2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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


using System;
using System.IO;

namespace Mavidian.DataConveyer.Logging
{
   /// <summary>
   /// A factory class to create logger instances.
   /// </summary>
   public static class LoggerCreator
   {

      /// <summary>
      /// The most recently created instance of ILogger (null if <see cref="CreateLogger"/> method was never called)
      /// </summary>
      internal static ILogger CurrentLogger { get; private set; }

      static LoggerCreator()  { CurrentLogger = null; }


      /// <summary>
      /// A factory method to create a logger instance.
      /// </summary>
      /// <param name="loggerType">Type of the logger to be created. If not specified, <see cref="LoggerType.Null"/> logger is assumed</param>
      /// <param name="logDescription">Description of the process being logged to be placed in the logger header.</param>
      /// <param name="loggingThreshold">The least severe severity level to be logged. If not specified, severity level of <see cref="LogEntrySeverity.None"/> is assumed.</param>
      /// <param name="loggerInfo">Additional information required to initialize the logger; specific to a particular logger type. In case of a <see cref="LoggerType.LogFile"/> logger, it is a path to the log file (if LogFile path absent, then DataConveyer.log file in the current folder will be assumed).</param>
      /// <returns>The newly created logger instance.</returns>
      public static ILogger CreateLogger(LoggerType loggerType = LoggerType.Null,
                                         string logDescription = "",
                                         LogEntrySeverity loggingThreshold = LogEntrySeverity.None,
                                         string loggerInfo = null)
      {
         switch (loggerType)
         {
            case LoggerType.Null:
               return CurrentLogger = new NullLogger();
            case LoggerType.LogFile:
               StreamWriter logWriter;
               try { logWriter = File.AppendText(loggerInfo ?? "DataConveyer.log"); }
               catch (Exception) { return CurrentLogger = new NullLogger(); }  //not much we can do other than downgrade to null logger
               return CurrentLogger = new FileLogger(loggingThreshold, logDescription, logWriter);
            case LoggerType.NLog:
               throw new NotImplementedException("NLog logger type has not yet been implemented.");
            default:
               return CurrentLogger = new NullLogger();
         }
      }
   }
}
