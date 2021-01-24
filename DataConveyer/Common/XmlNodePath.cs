//XmlNodePath.cs
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


using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Common
{
   /// <summary>
   /// Represents a relative xpath where element at every level is defined (no //'s).
   /// Contains a list of XmlNodeDef objects.
   /// </summary>
   internal class XmlNodePath
   {
      private readonly List<XmlNodeDef> _nodeDefs;  //may be null

      internal XmlNodePath(string specs)
      {
         _nodeDefs = specs?.Split('/')  //_nodeDefs is null if null specs
                          .Where(nd => !string.IsNullOrEmpty(nd))  //exclude consecutive / in specs (undefined levels)
                          .Where(nd => nd != ".")                  //exclude self-reference
                          .Select(nd => new XmlNodeDef(nd))
                          .ToList();
      }

      internal XmlNodePath(IEnumerable<XmlNodeDef> nodeDefs)
      {
         _nodeDefs = nodeDefs?.ToList();
      }

      internal IReadOnlyList<XmlNodeDef> NodeDefs { get { return _nodeDefs; } }

      internal bool IsEmpty { get { return _nodeDefs == null || !_nodeDefs.Any(); } }
   }
}
