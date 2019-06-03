//LineDispenser.cs
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

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// Output consumer that sends output lines to text files.
   /// </summary>
   internal class LineDispenser : ILineDispenser
   {
      private readonly IList<LineDispenserForTarget> _constituentDispensers;

      /// <summary>
      /// Creates an instance for a given set of writers to text files.
      /// </summary>
      /// <param name="constituentDispensers"></param>
      internal LineDispenser(IList<LineDispenserForTarget> constituentDispensers)
      {  //note IList (not IEnumerable) to prevent multiple querying, e.g. during disposing  
         _constituentDispensers = constituentDispensers;
      }  //ctor


      /// <summary>
      /// Send output line to the appropriate text file.
      /// </summary>
      /// <param name="linePlus">Tuple containing text line and target number that indicates the file to send it to.</param>
      public void SendNextLine(Tuple<ExternalLine, int> linePlus)
      {
         if (linePlus == null)
         {  //end of data; clear all buffers (in case caller forgets to call dispose)
            foreach (var target in _constituentDispensers)
            {
               target.ConcludeDispensing();
            }
         }
         else
         {
            _constituentDispensers[linePlus.Item2 - 1].SendNextLine(linePlus);
         }
      }


      /// <summary>
      /// Asynchronously send output line to the appropriate text file.
      /// </summary>
      /// <param name="linePlus">Tuple containing text line and target number that indicates the file to send it to.</param>
      /// <returns></returns>
      public async Task SendNextLineAsync(Tuple<ExternalLine, int> linePlus)
      {
         if (linePlus == null)
         {  //end of data; clear all buffers (in case caller forgets to call dispose)
            foreach (var target in _constituentDispensers)
            {
               await target.ConcludeDispensingAsync();
            }
         }
         else
         {
            await _constituentDispensers[linePlus.Item2 - 1].SendNextLineAsync(linePlus);
         }
      }


      public void Dispose()
      {
         foreach (var target in _constituentDispensers)
         {
            target.Dispose();
         }
      }
   }
}
