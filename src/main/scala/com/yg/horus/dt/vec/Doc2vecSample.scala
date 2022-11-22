package com.yg.horus.dt.vec

import org.deeplearning4j
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer
import org.deeplearning4j.models.paragraphvectors._
import org.deeplearning4j.text.documentiterator.{LabelledDocument, SimpleLabelAwareIterator}
import org.deeplearning4j.text.tokenization.tokenizerfactory.{DefaultTokenizerFactory, TokenizerFactory}

import java.io.File
import scala.jdk.CollectionConverters.{asScalaBufferConverter, seqAsJavaListConverter}

class Doc2vecSample {
  def train(dataset: List[String], labels: Option[List[String]]) = {
    val tokenizer: TokenizerFactory = new DefaultTokenizerFactory()
    val labelsUsed = collection.mutable.ListBuffer.empty[String]

    // Create the labeled documents, unique label for each document
    val docs = dataset.zipWithIndex.map(docWithIndex => {
      val label = labels match {
        case Some(lbl) => lbl(docWithIndex._2)
        case None => "SENT_" + docWithIndex._2
      }
      labelsUsed += label
      val doc = new LabelledDocument
      doc.setContent(docWithIndex._1)
      doc.addLabel(label)
      doc
    }).asJava

    // Build iterator for the dataset
    val iterator = new SimpleLabelAwareIterator(docs)

    // Determine if we need to load existing word vectors or not
//    val word2vec = WordVectorSerializer.loadGoogleModelNonNormalized(new File(w2vModelName), false, false))
//
//    val parVecs = word2vec match {
//      case Some(w) => new ParagraphVectors.Builder()
//        .minWordFrequency(minWordFrequency)
//        .iterations(iterations)
//        .epochs(epochs)
//        .layerSize(layerSize)
//        .learningRate(learningRate)
//        .windowSize(windowSize)
//        .batchSize(batchSize)
//        .iterate(iterator)
//        .trainWordVectors(trainWordVectors)
//        .sampling(sampling)
//        .tokenizerFactory(tokenizer)
//        .useExistingWordVectors(word2vec)
//        .build()
//
//        parVecs.fit() // This is where it goes wrong
    }
  }
}
object Doc2vecSample {
  def main(args: Array[String]): Unit = {

  }
}
