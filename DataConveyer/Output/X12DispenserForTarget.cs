//X12DispenserForTarget.cs
//
// Copyright © 2017-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.
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
   /// X12 segment writer (to a single target).
   /// </summary>
   internal class X12DispenserForTarget : LineDispenserForTarget
   {
      private readonly Lazy<string> _x12SegmentDelimiter;

      /// <summary>
      /// Creates an instance of a segment dispenser for a single target.
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="targetNo">1 based target number.</param>
      /// <param name="x12SegmentDelimiter">Segment delimiter in case of X12 output (may include whitespace, e.g. "~\r\n"); null otherwise.</param>
      internal X12DispenserForTarget(TextWriter writer, int targetNo, Lazy<string> x12SegmentDelimiter) : base(writer, targetNo)
      {
         //Segment delimiter is Lazy, so that it is not evaluated until after the first input row is read; this way, it can be
         // copied from the intake is case both intake and output are X12.
         _x12SegmentDelimiter = new Lazy<string>(() => x12SegmentDelimiter.Value ?? "~");  //note that even for X12 output, delimiter may remain undefined (if no X12 intake and no default in config)
      }


      /// <summary>
      /// Send a single line to the target file.
      /// </summary>
      /// <param name="linePlus">Line of text to send along with target number.</param>
      public override void SendNextLine(Tuple<ExternalLine, int> linePlus)
      {
         Debug.Assert(linePlus.Item2 == TargetNo);  //note that LineDispenserForTarget class could've just been sent ExternalLine (and not the Tuple with TargetNo), but it's kept for consistency/duality with LineFeederForSource
         //TODO: Verify that line contains no segment delimiter (if it does, log error)
         _writer.Write(linePlus.Item1?.Text + _x12SegmentDelimiter.Value);  //in case of X12: line == segment
      }


      /// <summary>
      /// Asynchronously send a single line to the target file.
      /// </summary>
      /// <param name="linePlus">Line of text to send along with target number.</param>
      /// <returns></returns>
      public override async Task SendNextLineAsync(Tuple<ExternalLine, int> linePlus)
      {
         Debug.Assert(linePlus.Item2 == TargetNo);  //note that LineDispenserForTarget class could've just been sent ExternalLine (and not the Tuple with TargetNo), but it's kept for consistency/duality with LineFeederForSource
         //TODO: Verify that line contains no segment delimiter (if it does, log error)
         await _writer.WriteAsync(linePlus.Item1?.Text + _x12SegmentDelimiter);
      }

   }
}
