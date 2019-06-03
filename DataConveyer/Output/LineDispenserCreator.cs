//LineDispenserCreator.cs
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
using System.IO;
using System.Linq;

namespace Mavidian.DataConveyer.Output
{
   /// <summary>
   /// A factory of text dispenser objects (wrappers over writers to text files)
   /// </summary>
   internal static class LineDispenserCreator
   {
      /// <summary>
      /// Creates LineDispenser for a collection of file names and boolean flags
      /// </summary>
      /// <param name="files"></param>
      /// <param name="outputDataKind"></param>
      /// <param name="outputIsAsync"></param>
      /// <param name="x12SegmentDelimiter">Segment delimiter in case of X12 output (may include whitespace, e.g. "~\r\n"); null otherwise.</param>
      /// <param name="xmlSettings"></param>
      /// <returns></returns>
      internal static ILineDispenser CreateLineDispenser(IEnumerable<string> files, KindOfTextData outputDataKind, bool outputIsAsync, Lazy<string> x12SegmentDelimiter, string xmlSettings)
      {
         return CreateLineDispenser(files.Select(f => File.CreateText(f)).ToList(), outputDataKind, outputIsAsync, x12SegmentDelimiter, xmlSettings);  // CreateText may throw
         // Note the ToList above; its purpose is to force eager evaluation, so that exception (e.g. UnauthorizedAccessException) is thrown (& caught)
         // during OutputProvider initialization (InitOutput method); otherwise, the exception would've been deferred until start of writing records.
      }


      /// <summary>
      /// Creates LineDispenser for a collection of text writers (generates sequential target numbers).
      /// </summary>
      /// <param name="writers"></param>
      /// <param name="outputDataKind"></param>
      /// <param name="outputIsAsync"></param>
      /// <param name="x12SegmentDelimiter">Segment delimiter in case of X12 output (may include whitespace, e.g. "~\r\n"); null otherwise.</param>
      /// <param name="xmlSettings"></param>
      /// <returns></returns>
      internal static ILineDispenser CreateLineDispenser(IEnumerable<TextWriter> writers, KindOfTextData outputDataKind, bool outputIsAsync, Lazy<string> x12SegmentDelimiter, string xmlSettings)
      {
         int targetNo = 1;
         return new LineDispenser(writers.Select(w =>
         {
            var dispTarget = CreateLineDispenser(w, targetNo++, outputDataKind, outputIsAsync, x12SegmentDelimiter, xmlSettings);
            return dispTarget;
         }).ToList());
         //Here, ToList is needed to prevent multiple iterations over writers (which would've messed up targetNo closure); besides LineDispenser ctor demands IList (not IEnumerable)
      }


      /// <summary>
      /// Helper method to create constituent dispenser: arbitrary writer, target number, X12 indicator provided
      /// (this method is internal to facilitate unit tests)
      /// </summary>
      /// <param name="writer"></param>
      /// <param name="targetNo"></param>
      /// <param name="outputDataKind"></param>
      /// <param name="outputIsAsync"></param>
      /// <param name="x12SegmentDelimiter"></param>
      /// <param name="xmlSettings"></param>
      /// <returns></returns>
      private static LineDispenserForTarget CreateLineDispenser(TextWriter writer, int targetNo, KindOfTextData outputDataKind, bool outputIsAsync, Lazy<string> x12SegmentDelimiter, string xmlSettings)
      {
         if (outputDataKind.ExternalLineType() == ExternalLineType.Xtext) return new TextDispenserForTarget(writer, targetNo);
         if (outputDataKind == KindOfTextData.X12) return new X12DispenserForTarget(writer, targetNo, x12SegmentDelimiter);
         if (outputDataKind == KindOfTextData.XML) return new XmlDispenserForTarget(writer, targetNo, xmlSettings, outputIsAsync);
         if (outputDataKind == KindOfTextData.JSON) return new JsonDispenserForTarget(writer, targetNo, xmlSettings, outputIsAsync);
         throw new NotSupportedException($"Dispenser type for {outputDataKind} could not be determined.");
      }
   }
}
