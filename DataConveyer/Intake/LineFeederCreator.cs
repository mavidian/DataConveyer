//LineFeederCreator.cs
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
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// A factory of text feeder objects (i.e. those that implement ILineFeeder interface)
   /// </summary>
   internal static class LineFeederCreator
   {
      /// <summary>
      /// Create a feeder based on file names.
      /// </summary>
      /// <param name="files"></param>
      /// <param name="inputDataKind"></param>
      /// <param name="intakeIsAsync"></param>
      /// <param name="skipRepeatedHeaders"></param>
      /// <param name="x12SegmentDelimiter">non-default char means X12 mode</param>
      /// <param name="xmlJsonSettings"></param>
      /// <returns></returns>
      internal static ILineFeeder CreateLineFeeder(IEnumerable<string> files, KindOfTextData inputDataKind, bool intakeIsAsync, bool skipRepeatedHeaders, string x12SegmentDelimiter, string xmlJsonSettings)
      {
         return CreateLineFeeder(files.Select(f => File.OpenText(f)).ToList(), inputDataKind, intakeIsAsync, skipRepeatedHeaders, x12SegmentDelimiter, xmlJsonSettings);  // OpenText may throw
         // Note the ToList above; its purpose is to force eager evaluation, so that exception (e.g. FileNotFoundException) is thrown (& caught)
         // during IntakeProvider initialization (InitIntake method); otherwise, the exception would've been deferred until start of reading records.
      }


      /// <summary>
      /// Create feeder based on arbitrary set of readers.
      /// </summary>
      /// <param name="readers"></param>
      /// <param name="inputDataKind"></param>
      /// <param name="intakeIsAsync"></param>
      /// <param name="skipRepeatedHeaders"></param>
      /// <param name="x12SegmentDelimiter">Any non-default char means X12, i.e. segments</param>
      /// <param name="xmlJsonSettings"></param>
      /// <returns></returns>
      internal static ILineFeeder CreateLineFeeder(IEnumerable<TextReader> readers, KindOfTextData inputDataKind, bool intakeIsAsync, bool skipRepeatedHeaders, string x12SegmentDelimiter, string xmlJsonSettings)
      {
         int sourceNo = 1;
         bool skipHeader = false;  // false for 1st source, same as skipRepeatedHeaders for remaining sources
         return new LineFeeder(readers.Select(r =>
         {
            var feeder = CreateLineFeeder(r, sourceNo++, inputDataKind, intakeIsAsync, skipHeader, x12SegmentDelimiter, xmlJsonSettings);
            skipHeader = skipRepeatedHeaders;
            return feeder;
         }).ToList());
         //Here, ToList is needed to prevent multiple iterations over readers (which would've messed up sourceNo closure); besides LineFeeder ctor demands IList (not IEnumerable)
      }


      /// <summary>
      /// Helper method to create a single constituent feeder.
      /// </summary>
      /// <param name="reader"></param>
      /// <param name="sourceNo"></param>
      /// <param name="inputDataKind"></param>
      /// <param name="intakeIsAsync"></param>
      /// <param name="skipHeader"></param>
      /// <param name="x12SegmentDelimiter"></param>
      /// <param name="xmlJsonSettings"></param>
      /// <returns></returns>
      private static LineFeederForSource CreateLineFeeder(TextReader reader, int sourceNo, KindOfTextData inputDataKind, bool intakeIsAsync, bool skipHeader, string x12SegmentDelimiter, string xmlJsonSettings)
      {
         if (inputDataKind.ExternalLineType() == ExternalLineType.Xtext) return new TextFeederForSource(reader, sourceNo, skipHeader);
         if (inputDataKind == KindOfTextData.X12) return new X12FeederForSource(reader, sourceNo, x12SegmentDelimiter);
         if (inputDataKind == KindOfTextData.XML) return new XmlFeederForSource(reader, sourceNo, xmlJsonSettings, intakeIsAsync);
         if (inputDataKind == KindOfTextData.JSON) return new JsonFeederForSource(reader, sourceNo, xmlJsonSettings, intakeIsAsync);
         if (inputDataKind == KindOfTextData.UnboundJSON) return new UnboundJsonFeederForSource(reader, sourceNo, xmlJsonSettings);
         throw new NotSupportedException($"Feeder type for {inputDataKind} could not be determined.");
      }
   }
}
