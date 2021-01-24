//XmlNodeDef.cs
//
// Copyright © 2018-2021 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;

namespace Mavidian.DataConveyer.Common
{
   /// <summary>
   /// Defines XML node to facilitate writing XML to output or parsing XML input.
   /// Basically consists of the node name and the list of its attributes.
   /// </summary>
   internal class XmlNodeDef
   {
      private class AttributeColl : KeyedCollection<string, Tuple<string, object>>
      {
         protected override string GetKeyForItem(Tuple<string, object> item) { return item.Item1; }
      }

      internal readonly string Name;
      private readonly AttributeColl _attributes;

      /// <summary>
      /// Create an instance based on a simplified xpath fragment.
      /// </summary>
      /// <param name="specs">An xpath fragment (single node) always starting with a node name and optionally defining attributes, e.g. Member{@ID][@Name="Smith"].</param>
      internal XmlNodeDef(string specs)
      {
         //Name is the specs part before [ if any
         var name = Regex.Match(specs, @"[^\[]*").Value;
         Name = string.IsNullOrWhiteSpace(name) ? "__undetermined__" : name;

         _attributes = new AttributeColl();
         //Attributes use xpath syntax, e.g. [@..] e.g. [@id][@key="val"]  (multiple [] pair are supported; the =val part is optional val may or may not be quoted
         foreach (Match attrMatch in Regex.Matches(specs, @"\[@(\w+)\s*(=\s*""?([^""]*)""?)?\]"))
         {
            var attrPieces = attrMatch.Groups;
            Debug.Assert(attrPieces.Count == 4);  //0=whole match, 1=name, 2=optional value part, 3=value
            //TODO: Consider support for other data types and not just string (how to detect the type of attrPieces[3].Value?)
            AddAttribute(attrPieces[1].Value, attrPieces[2].Success ? attrPieces[3].Value : null);
         }
      }

      /// <summary>
      /// Add an attribute for a given name and value; do nothing if attribute already exists.
      /// </summary>
      /// <param name="name"></param>
      /// <param name="value"></param>
      internal void AddAttribute(string name, object value)
      {

         try { _attributes.Add(Tuple.Create(name, value)); }
         catch (ArgumentException) {  };  //"An item with the same key has already been added."
      }

      /// <summary>
      /// Return a list of all attributes
      /// </summary>
      /// <returns>A list of all attributes, or empty list if no attributes.</returns>
      internal List<Tuple<string, object>> GetAttributes()
      {
         return _attributes.ToList();
      }

      /// <summary>
      /// Return all attributes in a form of a name-value dictionary.
      /// Suitable for matching the attributes during parsing.
      /// </summary>
      /// <returns></returns>
      internal IDictionary<string, object> GetAttributeDict()
      {
         return _attributes.ToDictionary(a => a.Item1, a => a.Item2);
      }

      /// <summary>
      /// Return a value of a given attribute or null if the attribute is not present.
      /// </summary>
      /// <param name="name"></param>
      /// <returns></returns>
      internal object GetAttributeValue(string name)
      {
         try { return _attributes[name].Item2; }
         catch (KeyNotFoundException) { return null; }  //"The given key was not present in the dictionary."
         //Note that returned null can either mean "attribute with no value" or "no attribute; use AttributeExists to disambiguate.
      }

      /// <summary>
      /// Indicates whether a given attribute is present.
      /// </summary>
      /// <param name="name"></param>
      /// <returns></returns>
      internal bool AttributeExists(string name)
      {
         return _attributes.Contains(name);
      }
   }
}
