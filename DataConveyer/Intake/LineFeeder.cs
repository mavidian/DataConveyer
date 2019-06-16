//LineFeeder.cs
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


using Mavidian.DataConveyer.Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Intake
{
   //TODO: Consider System.Interactive.Async (Ix Async) package

   /// <summary>
   /// Intake supplier that retrieves input lines from text files.
   /// </summary>
   internal class LineFeeder : ILineFeeder
   {
      //Note that this class works only as expected if constituent feeders are of LineFeederForSource type (and not LineFeeder (recursive))
      private readonly IList<LineFeederForSource> _constituentFeeders;
      private readonly IEnumerator<Tuple<ExternalLine, int>> _iterator;
      private readonly IEnumerator<Task<Tuple<ExternalLine, int>>> _asyncIterator;

      /// <summary>
      /// Create a LineFeeder from a given set of constituent feeders (sources).
      /// </summary>
      /// <param name="constituentFeeders"></param>
      internal LineFeeder(IList<LineFeederForSource> constituentFeeders)
      {  //note IList (not IEnumerable) to prevent multiple querying, e.g. during disposing  
         _constituentFeeders = constituentFeeders;
         _iterator = GetAllLines().GetEnumerator();
         _asyncIterator = GetAsyncAllLines().GetEnumerator();
      }


      /// <summary>
      /// Return all lines (along with source numbers) for all constituent feeders.
      /// </summary>
      /// <returns></returns>
      private IEnumerable<Tuple<ExternalLine, int>> GetAllLines()
      {
         foreach (var feeder in _constituentFeeders)
         {
            foreach (var line in GetAllLines(feeder))
            {
               yield return line;
            }
         }
      }

      /// <summary>
      /// Return all text lines (accompanied by source numbers) for a given feeder.
      /// </summary>
      /// <param name="feeder"></param>
      /// <returns></returns>
      private IEnumerable<Tuple<ExternalLine, int>> GetAllLines(LineFeederForSource feeder)
      {
         Tuple<ExternalLine, int> line;
         while ((line = feeder.GetNextLine()) != null)
         {
            yield return line;
         }
      }


      /// <summary>
      /// Return sequence of tasks with all lines (along with source numbers) for all constituent feeders.
      /// The sequence may contain tasks with nulls in it (at end of each constituent feeder).
      /// There are two consecutive nulls at the very end.
      /// </summary>
      /// <returns></returns>
      private IEnumerable<Task<Tuple<ExternalLine, int>>> GetAsyncAllLines()
      {
         //Note that it only works as expected if _constituentFeeders is of LineFeederForSource type. Asynchronous use of nested LineFeeder objects
         // is not advised (there would be multiple nulls at end of nesting levels). 
         foreach (var feeder in _constituentFeeders)
         {
            foreach (var linePlus in GetAsyncAllLines(feeder))
            {
               yield return linePlus;
            }
         }
      }

      /// <summary>
      /// Return sequence of tasks with all text lines (accompanied by source numbers) for a given feeder.
      /// The sequence has an extra EOD element (null) at end.
      /// </summary>
      /// <param name="feeder"></param>
      /// <returns></returns>
      private IEnumerable<Task<Tuple<ExternalLine, int>>> GetAsyncAllLines(LineFeederForSource feeder)
      {
         while (true)
         {
            var linePlus = feeder.GetNextLineAsync();
            yield return linePlus;
            //Note that yield cannot be combined with await (hence we need the feeder to return the entire value to be returned, i.e. the tuple).
            //For this reason, the Task returned from GetNextLineAsync (unlike return value from synchronous GetNextLine) provides no clue
            // as to whether it was the last value (null) or not; this can only be determined after the async call completes, i.e.
            // after the tasks's result becomes available. Therefore, the sequence returned by this method contains the EOD mark (tuple with null),
            // which is unlike the "synchronous" GetAllLines method (which removes the EOD mark that terminates the sequence).
            //Note that the Result below will not block (as long as the consumer of this method processes the task result before requesting next item). 
            if (linePlus.Result.Item1 == null) yield break;
            //Also note that System.Interactive.Async (Ix Async) package might be helpful in elimination of sending the tuples containing nulls.
         }
      }


      /// <summary>
      /// Return next text line (along with source number) starting from 1st source, then 2nd source, etc.
      /// </summary>
      /// <returns>Next available line (along with source number) or null at the end of the last source.</returns>
      public Tuple<ExternalLine, int> GetNextLine()
      {
         if (_iterator.MoveNext())
         {
            return _iterator.Current;
         }
         //no more lines
         return null;
      }


      /// <summary>
      /// Return task with the next text line (along with source number) starting from 1st source, then 2nd source, etc.
      /// </summary>
      /// <returns> Task with next available line (along with source number), null with source number (i.e. Tuple&lt;null..) at end of each source and null after the end of last source.
      /// So, at the very end, there will be Tuple containing null followed by a null tuple. </returns>
      public Task<Tuple<ExternalLine, int>> GetNextLineAsync()
      {
         if (_asyncIterator.MoveNext())
         {
            return _asyncIterator.Current;
         }
         //no more (tasks with) lines
         return Task.FromResult<Tuple<ExternalLine, int>>(null);  //at the very end we return a null tuple (in contrast with a tuple that contains null at end of each source)
      }


      public void Dispose()
      {
         foreach (var feeder in _constituentFeeders)
         {
            feeder.Dispose();
         }
      }
   }
}
