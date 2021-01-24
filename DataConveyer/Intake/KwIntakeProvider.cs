//KwIntakeProvider.cs
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
   /// <summary>
   /// A set of functions specific to KW data to be supplied to the Intake part of the EtlOrchestrator (Strategy Pattern)
   /// </summary>
   internal class KwIntakeProvider : IntakeProvider
   {
      private readonly Regex _keywordSplitRegex;

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="config"></param>
      /// <param name="globalCache"></param>
      /// <param name="typeDefinitions"></param>
      internal KwIntakeProvider(OrchestratorConfig config, IGlobalCache globalCache, TypeDefinitions typeDefinitions) : base(config, globalCache, typeDefinitions)
      {
         //Keyword token is in a form of key=value (value may be quoted, no spaces allowed except in quoted value)
         //Examples: @NUM=123              (Key:NUM, Val:123)
         //          @pNAME =  "Mary Ann"  (Key:NAME, Val:Mary Ann) - if quoted whitespace in front of quote will be trimmed 

         var sep = _config.InputFieldSeparator;
         _keywordSplitRegex = new Regex("(?<=" + sep + "|^)(?:[\\s-[" + sep + "]]*)([^" + sep + "=\\s]+(=(?:[\\s-[" + sep + "]]*)([^\"" + sep + "]+|\"(?:[^\"]|\"\")*\"|))?)", RegexOptions.Compiled);

         //Examples:
         //var tokenPattern = @"(?<=,|^)(?:[\s-[,]]*)([^,=\s]+(=(?:[\s-[,]]*)([^"",]+|""(?:[^""]|"""")*""|))?)";  //comma-delimited
         //var tokenPattern = "(?<=\\||^)(?:[\\s-[\\|]]*)([^\\|=\\s]+(=(?:[\\s-[\\|]]*)([^\"\\|]+|\"(?:[^\"]|\"\")*\"|))?)";  //pipe-delimited
         //var tokenPattern = "(?<=\\t|^)(?:[\\s-[\\t]]*)([^\\t=\\s]+(=(?:[\\s-[\\t]]*)([^\"\\t]+|\"(?:[^\"]|\"\")*\"|))?)";  //tab-delimited
         //Explanation (based on tab-delimited):
         // (?<=\t|^)           - positive lookbehind: must be preceded by either separator or start of line
         // (?:[\s-[\t]]*)      - do not capture (i.e. ignore) any whitespace (except for separator, in this case tab) at the beginning of the token
         // ( .... )            - group 1 of this regex (extends to the very end of the pattern) to be picked by TokenizeLineUsingRegex (hence 2nd parm below is 1)
         // [^\t=\s]+           - one or more any characters but separator, equal sign ory whitespace (will constitute a key)
         // (=...( .. | .. |))? - equal sign (optionally followed by ... i.e. whitespace to be ignored) followed by either of the ..'s below (will constitute a value) or equal sign alone; the entire thing incl. equal sign is optional
         // (?:[\s-[\t]]*)      - do not capture (i.e. ignore) any whitespace (except for separator, in this case tab) immediately following the equal sign
         // [^"\t]+             - anything but quote or separator (one or more)
         // "(?:[^"]|"")*"      - quoted string (may contain dual quotes inside)
      }


      /// <summary>
      /// Function to extract individual key value pairs (tokens) from input line
      /// Each token (in the key=value format) will be processed by ItemFromToken function to construct the KeyValItem
      /// </summary>
      internal override Func<ExternalLine, IEnumerable<string>> FieldTokenizer
      {
         get
         {
            return line => { return line.Text.TokenizeLineUsingRegex(_keywordSplitRegex, 1); };
         }
      }


      /// <summary>
      /// Function to create a KeyValItem for a given token obtained from TokenizeKwLine function
      /// </summary>
      internal override Func<string, int, IItem> ItemFromToken
      {
         get
         {
            return (t, i) => { return ItemFromKwToken(t, base._typeDefinitions); };
         }
      }


      internal override Func<Tuple<string, object>, IItem> ItemFromExtItem => t => throw new InvalidOperationException("KwIntakeProvider must not use ItemFromExtItem function.");

   }
}
