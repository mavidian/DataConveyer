//DelimitedIntakeProvider.cs
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
using System.Text.RegularExpressions;

namespace Mavidian.DataConveyer.Intake
{
   internal class DelimitedIntakeProvider : IntakeProvider
   {
      private readonly Regex _delimitedSplitRegex;

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      internal DelimitedIntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions) : base(config,  globalCache, typeDefinitions)
      {
         // This pattern matches all delimited tokens from intake line
         // Tokens can be unquoted (no separators/quotes) or quoted (separators and quoted quotes OK).
         // Note that part of a quoted field after the closing quote is ignored.
         // The same pattern is used for header line (if applicable) as well as all data lines.

         var sep = _config.InputFieldSeparator;
         _delimitedSplitRegex = new Regex("(?<=" + sep + "|^)(?:[\\s-[" + sep + "]]*)([^\"" + sep + "]+|\"(?:[^\"]|\"\")*\")?", RegexOptions.Compiled);

         //Examples of tokenPattern"
         // Comma-delimited:   @"(?<=,|^)(?:[\s-[,]]*)([^"",]+|""(?:[^""]|"""")*"")?"    (note that for comma-delimited [\\s-[,]] is an "overkill" as it's the same as [\\s], i.e. \\s )
         // Tab-delimited:     "(?<=\\t|^)(?:[\\s-[\\t]]*)([^\"\\t]+|\"(?:[^\"]|\"\")*\")?"
         //Explanation (based on comma-delimited):
         // (?<=,|^)       - positive lookbehind: must be preceded by either separator or start of line
         // (?:[\s-[,]]*)  - do not capture (i.e. ignore) any whitespace (except for separator - note that separator may be whitespace, such as tab, in which case this exception is relevant)
         // ( .. | .. )?   - either of these ..'s below - this group 1 of the regex is to be picked by TokenizeLineUsingRegex
         // [^",]+         - anything but quote or separator (one or more)
         // "(?:[^"]|"")*" - quoted string (may contain dual quotes inside)
      }


      internal override Func<ExternalLine, IEnumerable<string>> FieldTokenizer
      {
         get
         {
            return line => line.Text.TokenizeLineUsingRegex(_delimitedSplitRegex, 1);
         }
      }


      internal override Func<string, int, IItem> ItemFromToken
      {
         get
         {
            return (t, i) => { return ItemFromTextToken(t, i, base._typeDefinitions); };
         }
      }


      internal override Func<Tuple<string, object>, IItem> ItemFromExtItem => t => throw new InvalidOperationException("DelimitedIntakeProvider must not use ItemFromExtItem function.");

   }
}
