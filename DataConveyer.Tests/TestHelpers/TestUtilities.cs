//EventRecorder.cs
//
// Copyright © 2016-2019 Mavidian Technologies Limited Liability Company. All Rights Reserved.using System;
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
using System.Text;
using System.Threading.Tasks.Dataflow;

namespace DataConveyer.Tests.TestHelpers
{
   internal static class TestUtilities
   {

      /// <summary>
      /// Helper method that creates an orchestrator which intercepts output from a given block (e.g. ClusteringBlock or HoldingBlock)
      /// and sends it directly to the results extractor.
      /// Call to this method should be the last step of the arrange part for each test.
      /// </summary>
      /// <typeparam name="T">The output type of the intercepted block, e.g. KeyValCluster.</typeparam>
      /// <param name="config">Orchestrator configuration to crate the orchestrator for.</param>
      /// <param name="nameOfBlockToIntercept">Name of the last block of the pipeline as it appears in EtlOrchestrator class, e.g "_clusteringBlock" or "_holdingBlock".
      /// This block must not be linked to other blocks.</param>
      /// <param name="resultsExtractor">Action block that will be receiving clusters instead of the rest of the pipeline.</param>
      /// <returns>The orchestrator just created.</returns>
      internal static EtlOrchestrator GetTestOrchestrator<T>(OrchestratorConfig config, string nameOfBlockToIntercept, ActionBlock<T> resultsExtractor)
      {
         var orchestrator = new EtlOrchestrator(config);
         var orchestratorPA = new PrivateAccessor(orchestrator);
         var blockToIntercept = (ISourceBlock<T>)orchestratorPA.GetField(nameOfBlockToIntercept);
         blockToIntercept.LinkTo(resultsExtractor, new DataflowLinkOptions { PropagateCompletion = true });
         return orchestrator;
      }
   }
}
