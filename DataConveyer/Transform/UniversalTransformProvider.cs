﻿//UniversalTransformProvider.cs
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
using Mavidian.DataConveyer.Orchestrators;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Mavidian.DataConveyer.Transform
{
   internal class UniversalTransformProvider : TransformProvider
   {

      /// <summary>
      /// Ctor intended to be called by the CreateProvider method of the base class
      /// </summary>
      /// <param name="typeDefinitions"></param>
      /// <param name="config"></param>
      internal UniversalTransformProvider(TypeDefinitions typeDefinitions, OrchestratorConfig config) : base(typeDefinitions, config) { }


      internal override Func<KeyValCluster, IEnumerable<KeyValCluster>> Transform
      {
         get
         {
            //Universal transformer simply passes input cluster to output
            return clstr => _config.UniversalTransformer(clstr).Select(c => (KeyValCluster)c);
         }
      }
   }
}
