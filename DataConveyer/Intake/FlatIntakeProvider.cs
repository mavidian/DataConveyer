//FlatIntakeProvider.cs
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
using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Mavidian.DataConveyer.Intake
{
   internal class FlatIntakeProvider : IntakeProvider
   {
      private readonly List<int> _fieldWidths;  //fixed field widths

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      internal FlatIntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions) : base(config, globalCache, typeDefinitions)
      {
         //In case field widths only are configured (no field names) and no names in the header row, allow assignment of default names:
         if (FieldsNamesFromConfig.IsEmptyList() && !_config.HeadersInFirstInputRow) IncludeFieldsEnMasse(FieldsNamesFromConfig);

         _fieldWidths = config.InputFields.ListOfSingleElements(1)
                               .ToListOfInts((config.DefaultInputFieldWidth == 0) ? 10 : config.DefaultInputFieldWidth)  //in case of undefined default width, 10 will be used
                               .ToList();
      }

      internal override Func<ExternalLine, IEnumerable<string>> FieldTokenizer
      {
         get
         {
            return line => TokenizeFixedWidthLine(line.Text, _fieldWidths);
         }
      }

      internal override Func<string, int, IItem> ItemFromToken
      {
         get
         {
            return (t, i) => { return ItemFromTextToken(t, i, base._typeDefinitions); };
         }
      }


      internal override Func<Tuple<string, object>, IItem> ItemFromExtItem => t => throw new InvalidOperationException("FlatIntakeProvider must not use ItemFromExtItem function.");


      /// <summary>
      /// Split line into tokens based on fixed width fields
      /// </summary>
      /// <param name="line">Input line</param>
      /// <param name="fieldWidths">Array of field widths</param>
      /// <returns></returns>
      private static IEnumerable<string> TokenizeFixedWidthLine(string line, List<int> fieldWidths)
      {
         if (fieldWidths.Count == 0) return Enumerable.Empty<string>();
         return TokenizeFixedWidthLine(line, 0, 0, fieldWidths);
      }

      /// <summary>
      /// Recursive helper function to  supply fixed width fields starting from a given field position
      /// </summary>
      /// <param name="line">Input line</param>
      /// <param name="fldNo">Zero-based number of current field (token)</param>
      /// <param name="fldStart">Zero-based starting position of current field (token) in input line</param>
      /// <param name="fieldWidths">Array of field widths</param>
      /// <returns></returns>
      private static IEnumerable<string> TokenizeFixedWidthLine(string line, int fldNo, int fldStart, List<int> fieldWidths)
      {
         Debug.Assert(fldNo < fieldWidths.Count);
         var currFldWidth = fieldWidths[fldNo];
         var retVal = line.SafeSubstring(fldStart, currFldWidth);
         yield return retVal;
         if (fldNo == fieldWidths.Count - 1) yield break;
         foreach (var t in TokenizeFixedWidthLine(line, ++fldNo, fldStart + currFldWidth, fieldWidths))
         {
            yield return t;
         }
      }

   }
}
