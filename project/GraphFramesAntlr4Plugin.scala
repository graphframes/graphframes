/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.antlr.v4.Tool
import sbt._
import sbt.Keys._

/**
 * Minimal internal sbt plugin that generates ANTLR4 Java sources from `.g4`
 * grammar files.
 *
 * It exposes a single task, [[GraphFramesAntlr4Plugin.autoImport.antlr4Generate]],
 * which invokes the ANTLR4 tool (resolved as a compile-time dependency of the
 * meta-build in `project/plugins.sbt`) to emit Java sources for a lexer and a
 * parser grammar.
 *
 * Design notes:
 *   - The ANTLR4 tool is a build-time-only dependency; the antlr4-runtime is
 *     provided transitively by Spark SQL at runtime, so no new dependency is
 *     added to the application build.
 *   - The parser is generated with `-visitor -no-listener`: the AST is built
 *     with a visitor (the AstBuilder extends the generated `*BaseVisitor`), so
 *     the default listener would be dead weight.
 *   - Generated sources should be emitted into a named package (set
 *     `antlr4GenPackage`); otherwise the unnamed-package classes cannot be
 *     imported from packaged Scala/Java sources.
 *   - The lexer must be generated before the parser, because the parser grammar
 *     references the lexer via `options { tokenVocab = GqlLexer; }`. We point
 *     ANTLR's `-lib` at the output directory so the freshly emitted
 *     `GqlLexer.tokens` is found.
 *   - The tool version is selected in `project/plugins.sbt` and must match the
 *     antlr4-runtime bundled with the targeted Spark major (3.5.x -> 4.9.3,
 *     4.x -> 4.13.1); the generated ATN format is version-locked to it.
 *
 * The plugin is opt-in: enable it with `.enablePlugins(GraphFramesAntlr4Plugin)`.
 */
object GraphFramesAntlr4Plugin extends AutoPlugin {

  object autoImport {
    val antlr4Generate =
      TaskKey[Unit]("antlr4Generate", "Generate ANTLR4 Java sources from the .g4 grammar files.")
    val antlr4GrammarDir =
      SettingKey[File]("antlr4GrammarDir", "Directory containing the .g4 grammar files.")
    val antlr4LexerGrammar =
      SettingKey[File]("antlr4LexerGrammar", "Path to the lexer .g4 file.")
    val antlr4ParserGrammar =
      SettingKey[File]("antlr4ParserGrammar", "Path to the parser .g4 file.")
    val antlr4OutputDir =
      SettingKey[File]("antlr4OutputDir", "Output directory for generated ANTLR4 sources.")
    val antlr4GenPackage =
      SettingKey[Option[String]](
        "antlr4GenPackage",
        "Optional Java package to generate into (ANTLR -package option). None by default.")
  }

  import autoImport._

  override def requires: Plugins = sbt.plugins.JvmPlugin
  override def trigger: PluginTrigger = noTrigger

  override def projectSettings: Seq[Setting[_]] = Seq(
    // Defaults point at the conventional GraphFrames grammar location.
    antlr4GrammarDir := (Compile / baseDirectory).value / "src" / "main" / "antlr4" /
      "org" / "graphframes" / "propertygraph" / "internal",
    antlr4LexerGrammar := antlr4GrammarDir.value / "GqlLexer.g4",
    antlr4ParserGrammar := antlr4GrammarDir.value / "GqlParser.g4",
    antlr4OutputDir := target.value / "generated-sources" / "antlr4",
    antlr4GenPackage := None,
    antlr4Generate := {
      val log = streams.value.log
      val outDir = antlr4OutputDir.value
      val lexerG4 = antlr4LexerGrammar.value
      val parserG4 = antlr4ParserGrammar.value
      val pkgOpt = antlr4GenPackage.value

      if (!lexerG4.isFile) {
        sys.error(s"ANTLR4 lexer grammar not found: $lexerG4")
      }
      if (!parserG4.isFile) {
        sys.error(s"ANTLR4 parser grammar not found: $parserG4")
      }

      IO.createDirectory(outDir)
      // The parser imports the lexer's token vocab, so the lexer is generated
      // first; the generated GqlLexer.tokens then lives in the output dir.
      log.info(s"ANTLR4: generating lexer from ${lexerG4.getName} into $outDir")
      runAntlr(lexerG4, outDir, pkgOpt, libDir = None, genVisitor = false)
      log.info(s"ANTLR4: generating parser from ${parserG4.getName} into $outDir")
      runAntlr(parserG4, outDir, pkgOpt, libDir = Some(outDir), genVisitor = true)
    },
    // Register the generated Java as managed Compile sources, and regenerate
    // them automatically before compile. Returning the files from
    // sourceGenerators is what actually makes them part of the Compile sources
    // (the output dir is intentionally NOT added to unmanagedSourceDirectories
    // to avoid double-counting).
    Compile / sourceGenerators += Def.task {
      antlr4Generate.value
      (antlr4OutputDir.value ** "*.java").get
    }.taskValue,
  )

  /**
   * Invoke the ANTLR4 tool on a single grammar file. Throws on any error.
   *
   * @param grammar    the `.g4` file to process
   * @param outDir     the `-o` output directory
   * @param pkgOpt     optional `-package` argument
   * @param libDir     optional `-lib` directory (where to find imported `.tokens`)
   * @param genVisitor when true, emit a visitor and suppress the listener
   *                   (`-visitor -no-listener`). The AST is built with a visitor,
   *                   so the listener is dead weight. These options are
   *                   parser-grammar only; pass false for the lexer.
   */
  private def runAntlr(
      grammar: File,
      outDir: File,
      pkgOpt: Option[String],
      libDir: Option[File],
      genVisitor: Boolean): Unit = {
    val args = scala.collection.mutable.ArrayBuffer.empty[String]
    args += "-o"
    args += outDir.getAbsolutePath
    libDir.foreach { lib =>
      args += "-lib"
      args += lib.getAbsolutePath
    }
    pkgOpt.foreach { pkg =>
      args += "-package"
      args += pkg
    }
    if (genVisitor) {
      args += "-visitor"
      args += "-no-listener"
    }
    args += grammar.getAbsolutePath

    val tool = new Tool(args.toArray)
    // processGrammarsOnCommandLine returns normally even on grammar errors;
    // the error count tells us whether anything went wrong.
    tool.processGrammarsOnCommandLine()
    if (tool.getNumErrors > 0) {
      sys.error(s"ANTLR4 reported ${tool.getNumErrors} error(s) while processing ${grammar.getName}")
    }
  }
}
