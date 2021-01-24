//IntakeHelpers.cs
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


using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace Mavidian.DataConveyer.Intake
{
   /// <summary>
   /// Helper methods, such as extension methods to assist in intake processing
   /// </summary>
   internal static class IntakeHelpers
   {
      /// <summary>
      /// Lazily returns a sequence of lines from the input file
      /// </summary>
      /// <param name="inputFileName"></param>
      /// <returns></returns>
      internal static IEnumerable<string> GetInputLinesFromFile(string inputFileName)
      {
         //TODO: Message when file does not exist
         using (StreamReader reader = File.OpenText(inputFileName))
         {
            string line;
            while ((line = reader.ReadLine()) != null)
            {
               yield return line;
            }
         }
      }


      /// <summary>
      /// Split line into tokens based on provided regex pattern.
      /// </summary>
      /// <param name="line">Input line to be split.</param>
      /// <param name="regex">Regex that splits input line and captures a collection of tokens to be extracted.</param>
      /// <param name="groupToExtract">Index of the group (within the match) that contains the token to return.</param>
      /// <returns></returns>
      internal static IEnumerable<string> TokenizeLineUsingRegex(this string line, Regex regex, int groupToExtract)
      {
         foreach (Match m in regex.Matches(line))
         {
            yield return m.Groups[groupToExtract].Value;
         }
      }


      /// <summary>
      /// Split line into tokens based on provided array of regex patterns
      /// </summary>
      /// <param name="line">Input line</param>
      /// <param name="defs">Array of tuples consisting of field names and corresponding Regex patterns</param>
      /// <returns>A token in the form of key=value</returns>
      internal static IEnumerable<string> TokenizeUsingArbitraryDefs(this string line, IEnumerable<Tuple<string, string>> defs)
      {
         foreach (Tuple<string, string> def in defs)
         {
            var regex = GetMemoizedRegex(def.Item2);
            yield return def.Item1 + "=" + regex.Match(line).ToString();
         }
      }


      /// <summary>
      /// Get Regex object for a given regular expression
      /// </summary>
      /// <param name="expression">Regular expression</param>
      /// <returns>Regex object created for the first time (and cached, so that it is not recreated during subsequent calls)</returns>
      private static Regex GetMemoizedRegex(string expression)
      {
         var cache = new Dictionary<string, Regex>();
         if (cache.TryGetValue(expression, out Regex retVal)) return retVal;
         return cache[expression] = new Regex(expression);
      }


      /// <summary>
      /// Remove whitespace before opening quote in quoted string
      /// </summary>
      /// <param name="value"></param>
      /// <returns></returns>
      internal static string TrimInFrontOfQuote(this string value)
      {
         if (value.Length == 0) return value;
         var trimmedVal = value.TrimStart();
         return trimmedVal[0] == '"' ? trimmedVal : value;
      }


      /// <summary>
      /// Remove surrounding quotes and unescape inner quotes from a string value (unless retainQuotes is true)
      /// </summary>
      /// <param name="value"></param>
      /// <param name="retainQuotes"></param>
      /// <returns></returns>
      internal static string UnquoteIfNeeded(this string value, bool retainQuotes)
      {
         if (retainQuotes || value.Length == 0 || value[0] != '"')
         {
            return value;
         }
         else
         {
            //remove surrounding quotes from quoted string and unescape remaining quotes
            //Example: "Payton, Robert ""Bob""" -> Payton, Robert "Bob"
            return value.Substring(1, value.Length - 2).Replace("\"\"", "\"");
         }
      }


      /// <summary>
      /// Remove leading and trailing whitespace, but only if trimValues is true
      /// </summary>
      /// <param name="value"></param>
      /// <param name="trimValues">true to remove leading and trailing spaces from values; false to leave all values of fixed width</param>
      /// <returns></returns>
      internal static string TrimIfNeeded(this string value, bool trimValues)
      {
         if (trimValues) return value.Trim();
         return value;
      }

      /// <summary>
      ///Same as string.Substring, except no ArgumentOutOfRangeException thrown when the
      /// substring doesn't fit in the input string and instead the remainder of the string
      /// (possibly string.Empty) gets returned.
      /// </summary>
      /// <param name="input">Input string</param>
      /// <param name="startIndex">Zero-based starting character position</param>
      /// <param name="length"></param>
      /// <returns></returns>
      internal static string SafeSubstring(this string input, int startIndex, int length)
      {
         input = input ?? string.Empty;
         int altLength = input.Length;
         if (startIndex >= altLength) return string.Empty;
         altLength -= startIndex;
         return input.Substring(startIndex, length < altLength ? length : altLength);

         ////Alternative using Linq (slower?)
         //return new string((input ?? string.Empty).Skip(startIndex).Take(length).ToArray());
      }


      /// <summary>
      /// Split slash separated JSONnode path
      /// </summary>
      /// <param name="specs"></param>
      /// <returns></returns>
      internal static List<string> ToJsonNodePath(this string specs)
      {
         return specs?.Split('/')  //_nodeDefs is null if null specs
                      .ToList();
         //note that unlike XML, empty nodes are respected (object can be unnamed, i.e. StartObject token does not have to be preceded by PropertyName token)
      }

   }
}
