//LineFeederHelpers.cs
//
// Copyright © 2016-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// Extension methods to help reading data from text files
   /// </summary>
   internal static class LineFeederHelpers
   {
      /// <summary>
      /// Asynchronously get next line and wait for a task (with a Tuple) to complete, ignoring tuples with nulls at end of each source.  
      /// </summary>
      /// <param name="feeder">Feeder to read the line from (should be the 1st level LineFeeder, i.e. with LineFeederForSource as constituent feeders).</param>
      /// <returns>Tuple containing text line and a source number or null at end.</returns>
      internal static Tuple<ExternalLine, int> GetNextLineSynced(this ILineFeeder feeder)
      {
         var retVal = feeder.GetNextLineAsync().Result;
         // retVal here is null at the very end of the entire sequence produced by LineFeeder.
         // At end of each level of LineFeeder nesting (i.e. LineFeederForSource), it is a tuple containing null.
         // Note that this assumes a single level of nesting, where LineFeeder is fed by LineFeederForSource (and not by LineFeeders (recursively)).
         if (retVal == null) return null;
         return retVal.Item1 == null ? GetNextLineSynced(feeder) : retVal;
      }
   }
}
