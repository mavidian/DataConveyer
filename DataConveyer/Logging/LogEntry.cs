//LogEntry.cs
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
//  - enum LogEntrySeverity
//  - class LogEntry

using System;

namespace Mavidian.DataConveyer.Logging
{
   /// <summary>
   /// Type/level of the log entry (the higher value, the less severe message).
   /// </summary>
   public enum LogEntrySeverity
   {
      /// <summary>
      /// Indicates that no log entries are reported.
      /// </summary>
      None = 0,  // This value not assigned to individual entries, except for Log Title Box, i.e. log header.
      /// <summary>
      /// Process aborted, e.g. input file not found.
      /// </summary>
      Fatal = 1,
      /// <summary>
      /// Some data skipped, e.g. input record rejected due to dup key.
      /// </summary>
      Error = 2,
      /// <summary>
      /// Some condition repaired, e.g. dup key replaced by a default key.
      /// </summary>
      Warning = 3,
      /// <summary>
      /// No impact on processing, e.g. total records processed.
      /// </summary>
      Information = 4,
      /// <summary>
      /// Verbose message used for troubleshooting purposes.
      /// </summary>
      Debug = 5
   }

   /// <summary>
   /// Entry to be placed on a log.
   /// </summary>
   public class LogEntry
   {
      /// <summary>
      /// Type/level of the log entry, one of: Fatal, Error, Warning, Information or Debug.
      /// </summary>
      internal readonly LogEntrySeverity Severity;
      /// <summary>
      /// Function to return the text of the message to report.
      /// </summary>
      internal readonly Func<string> MessageOnDemand;  //Func<string> and not string to allow lazy evaluation (helpful if entries are dropped / not reported).
      /// <summary>
      /// Exception that caused the entry to be logged (null if the entry was not caused by an exception).
      /// </summary>
      internal readonly Exception Exception;

      internal LogEntry(LogEntrySeverity severity, Func<string> messageOnDemand, Exception exception = null)
      {
         Severity = severity;
         MessageOnDemand = messageOnDemand;
         Exception = exception;
      }
   }
}
