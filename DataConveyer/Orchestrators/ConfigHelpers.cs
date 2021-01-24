//ConfigHelpers.cs
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


using Mavidian.DataConveyer.Entities.KeyVal;
using Mavidian.DataConveyer.Intake;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;

namespace Mavidian.DataConveyer.Orchestrators
{
   /// <summary>
   /// Extension methods to facilitate retrieval of config data
   /// </summary>
   internal static class ConfigHelpers
   {
      /// <summary>
      /// Return a sequence of strings from a separated list
      /// </summary>
      /// <param name="separatedList">A string containing items (possibly quoted) separated by a separator</param>
      /// <param name="separator">A character separating items</param>
      /// <returns></returns>
      internal static IEnumerable<string> ToListOfStrings(this string separatedList, char separator)
      {
         foreach (var val in separatedList.Split(separator))
         {
            yield return val.UnquoteIfNeeded(false);
         }
      }

      /// <summary>
      /// Return a sequence of integers from a comma separated list
      /// </summary>
      /// <param name="commaSepList">String containing comma separated list of integer values</param>
      /// <param name="defaultValue">Value to use in case element cannot be parsed to integer</param>
      /// <returns></returns>
      internal static IEnumerable<int> ToListOfInts(this string commaSepList, int defaultValue)
      {
         //foreach (var val in commaSepList.TokenizeLineUsingRegex(CsvRegex))
         foreach (var val in commaSepList.Split(','))
         {
            if (int.TryParse(val.UnquoteIfNeeded(false), out int intVal)) yield return intVal;
            else yield return defaultValue;  //return defaultValue in case of bad value provided
         }
      }

      /// <summary>
      /// Return a sequence of integers from a sequence of strings
      /// </summary>
      /// <param name="listOfStrings">Sequence of string values to be converted to integers</param>
      /// <param name="defaultValue">Value to use in case element cannot be parsed to integer</param>
      /// <returns></returns>
      internal static IEnumerable<int> ToListOfInts(this IEnumerable<string> listOfStrings, int defaultValue)
      {
         foreach (var val in listOfStrings)
         {
            if (int.TryParse(val?.UnquoteIfNeeded(false), out int intVal)) yield return intVal;
            else yield return defaultValue;  //return defaultValue in case of bad value provided
         }
      }

      /// <summary>
      /// Read (the setting) from a given file and return it's entire contents (ignoring new lines)
      /// </summary>
      /// <param name="fileName"></param>
      /// <returns>Entire contents of the file (CR/LF stripped); null if file does not exists</returns>
      internal static string ReadSettingFromFile(this string fileName)
      {
         if (fileName == null) return null;

         try
         {
            using (TextReader reader = File.OpenText(fileName))
            {
               return reader.ReadToEnd().Replace("\r\n", string.Empty);
            }
         }
         catch (FileNotFoundException)
         {
            //TODO: Message that file is missing
            return null;
         }
      }


      /// <summary>
      /// Parse submitted type definitions from config and return a sequence of corresponding tuples (key, typeDef)
      /// </summary>
      /// <param name="typeDefinitions">Comma delimited set of pipe delimited type definitions</param>
      internal static IEnumerable<Tuple<string, ItemDef>> ParseTypeDefinitions(this string typeDefinitions)
      {
         //TODO: Allow formats to contain commas and pipes
         string[] defs = typeDefinitions.Split(',');
         foreach (var def in defs)
         {
            string[] elems = def.Split('|'); // key|type|format

            ItemType tp;  //Type: I=Int, D=DateTime, M=Decimal(aka Money), S=String
            if (elems.Length > 1)
            {
               switch (elems[1])
               {
                  case "I": tp = ItemType.Int; break;
                  case "B": tp = ItemType.Bool; break;
                  case "D": tp = ItemType.DateTime; break;
                  case "M": tp = ItemType.Decimal; break;
                  default: tp = ItemType.String; break;  //note that Void type cannot be stored in type definitions
               }
            }
            else
            {  //missing type element, assume string
               tp = tp = ItemType.String;
            }
            yield return Tuple.Create(elems[0], new ItemDef(tp, elems.Length > 2 ? elems[2] : string.Empty));  //if format missing, empty string is assumed
         }
      }


      /// <summary>
      /// Interpret config setting containing a set of comma-separated pairs (values in each pair separated by a pipe symbol) as two lists
      /// </summary>
      /// <param name="setting">Comma separated list of  items, each containing 2 pipe separated values)</param>
      /// <returns>A tuple containing 2 elements: a list of first elements of each pair and a list of 2nd elements of each pair</returns>
      internal static Tuple<List<string>, List<string>> SplitPairsToTwoLists(this string setting)
      {
         var pairs = setting.Replace("\r\n", string.Empty).Split(',').Select(pair =>
         {
            string[] elems = pair.Split('|');
            string elem1 = elems[0].Trim();
            string elem2 = elems.Length > 1 ? elems[1].Trim() : null;
            return new { Elem1 = elem1, Elem2 = elem2 };
         });

         return Tuple.Create(pairs.Select(pair => pair.Elem1).ToList(), pairs.Select(pair => pair.Elem2).ToList());
      }

      /// <summary>
      /// Interpret config setting containing a set of comma-separated pairs (values in each pair separated by a pipe symbol) as a list of tuples
      /// </summary>
      /// <param name="setting">Comma separated list of  items, each containing 2 pipe separated values)</param>
      /// <returns>A sequence of tuples, each containing first and 2nd elements of each pair</returns>
      internal static IEnumerable<Tuple<string, string>> SplitPairsToListOfTuples(this string setting)
      {
         foreach (var pair in setting.Replace("\r\n", string.Empty).Split(','))
         {
            string[] elems = pair.Split('|');
            string elem1 = elems[0];
            string elem2 = elems.Length > 1 ? elems[1] : null;
            yield return Tuple.Create(elem1, elem2);
         }
      }

      /// <summary>
      /// Extract a list of single elements from a config setting containing a set of comma-separated pairs (values in each pair are separated by a pipe symbol)
      /// </summary>
      /// <param name="setting"></param>
      /// <param name="elementIndex"> 0 to extract first element from each pipe separated pair, 1 to extract 2nd ...</param>
      /// <returns></returns>
      internal static IEnumerable<string> ListOfSingleElements(this string setting, int elementIndex)
      {
         if (setting == null) return Enumerable.Empty<string>();
         return setting.Replace("\r\n", string.Empty).Split(',').Select(pair =>  //note that string.Empty (unlike null) will return a single element
         {
            string[] elems = pair.Split('|');
            return elems.Length > elementIndex ? elems[elementIndex].Trim() : null;
         });
      }


      /// <summary>
      /// Verifies if the list of strings only contains empty elements
      /// </summary>
      /// <param name="list"></param>
      /// <returns>true if all elements are empty; false if at least one element is not empty</returns>
      internal static bool IsEmptyList(this IEnumerable<string> list)
      {
         if (list == null) return true;
         foreach (string element in list)
         {
            if (!string.IsNullOrWhiteSpace(element)) return false;
         }
         return true;
      }


      /// <summary>
      /// Verifies if the list of strings only contains non-empty elements
      /// </summary>
      /// <param name="list"></param>
      /// <returns>true if all elements are not empty; false if at least one element is empty, or the list is null or empty</returns>
      internal static bool IsNonEmptyList(this IEnumerable<string> list)
      {
         if (list == null) return false;
         if (!list.Any()) return false;
         foreach (string element in list)
         {
            if (string.IsNullOrWhiteSpace(element)) return false;
         }
         return true;
      }


      /// <summary>
      /// Serialize object to XML
      /// </summary>
      /// <param name="input">Object to serialize</param>
      /// <returns></returns>
      internal static string ToXML(this object input)
      {
         if (input == null) return null;

         using (var writer = new StringWriter())
         {
            var serializer = new XmlSerializer(input.GetType());
            serializer.Serialize(writer, input);
            return writer.ToString();
         }
      }


      /// <summary>
      /// Deserialize an XML string to an object of given type
      /// </summary>
      /// <typeparam name="T">Type to deserialize to</typeparam>
      /// <param name="xml">A valid deserialized string of type T object</param>
      /// <returns></returns>
      public static T ToObject<T>(this string xml)
      {
         if (string.IsNullOrWhiteSpace(xml)) return default;

         using (var reader = new StringReader(xml))
         {
            var serializer = new XmlSerializer(typeof(T));
            return (T)serializer.Deserialize(reader);
         }
      }


      /// <summary>
      /// Obtain a string value from a dictionary.
      /// </summary>
      /// <param name="settings">A dictionary with both key and value of string type</param>
      /// <param name="settingName">Same as dictionary key.</param>
      /// <returns>Value for a given key or null if key does not exist (or dictionary is null).</returns>
      internal static string GetStringSetting(this IDictionary<string, string> settings, string settingName)
      {
         if (settings == null) return null;
         if (settings.TryGetValue(settingName, out string retVal)) return retVal;
         return null;
      }

   }
}
