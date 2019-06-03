//LineDispenserForTarget.cs
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
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// Base writer to a single target (constituent dispenser).
   /// </summary>
   internal abstract class LineDispenserForTarget : IDisposable
   {
      protected readonly TextWriter _writer;
      protected readonly int TargetNo;

      /// <summary>
      /// Creates an instance given a stream writer (for a specific text file) and a target number.
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="targetNo">1 based target number.</param>
      internal LineDispenserForTarget(TextWriter writer, int targetNo)
      {
         _writer = writer;
         TargetNo = targetNo;
      }  //ctor


      /// <summary>
      /// Send a single line to the target file.
      /// </summary>
      /// <param name="linePlus">Line of text to send along with target number.</param>
      public virtual void SendNextLine(Tuple<ExternalLine, int> linePlus)
      {
         Debug.Assert(linePlus.Item2 == TargetNo);  //note that LineDispenserForTarget class could've just be sent ExternalLine (and not the Tuple with TargetNo), but it's kept for consistency/duality with LineFeederForSource
         _writer.WriteLine(linePlus.Item1?.Text);
      }


      /// <summary>
      /// Asynchronously send a single line to the target file.
      /// </summary>
      /// <param name="linePlus">Line of text to send along with target number.</param>
      /// <returns></returns>
      public virtual async Task SendNextLineAsync(Tuple<ExternalLine, int> linePlus)
      {
         Debug.Assert(linePlus.Item2 == TargetNo);  //note that LineDispenserForTarget class could've just be sent ExternalLine (and not the Tuple with TargetNo), but it's kept for consistency/duality with LineFeederForSource
         await _writer.WriteLineAsync(linePlus.Item1?.Text);
      }


      /// <summary>
      /// Intended to be called when the LineDispenser (owner) receives EOD mark.
      /// </summary>
      internal virtual void ConcludeDispensing()
      {
         _writer.Flush();
      }


      /// <summary>
      /// Intended to be called when the LineDispenser (owner) receives EOD mark.
      /// </summary>
      internal virtual async Task ConcludeDispensingAsync()
      {
         await _writer.FlushAsync();
      }


      public virtual void Dispose()
      {
         _writer.Dispose();
      }

   }
}
