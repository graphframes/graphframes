# Copied from https://github.com/dinoboff/github-tools/blob/master/src/github/tools/sphinx.py
#
# Copyright (c) 2009, Damien Lebrun
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification,
# are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice,
# this list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
# THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
:Description: Sphinx extension to remove leading under-scores from directories names in the html build output directory.
"""
import os
import shutil
from collections.abc import Callable
from typing import Any
from sphinx.application import Sphinx

def setup(app: Sphinx) -> None:
    """
    Add a html-page-context  and a build-finished event handlers
    """
    app.connect('html-page-context', change_pathto)
    app.connect('build-finished', move_private_folders)

def change_pathto(app: Sphinx,
                 pagename: str,
                 templatename: str,
                 context: dict[str, Any],
                 doctree: Any | None) -> None:
    """
    Replace pathto helper to change paths to folders with a leading underscore.
    """
    pathto: Callable = context.get('pathto')
    def gh_pathto(otheruri: str, *args: Any, **kw: Any) -> Any:
        if otheruri.startswith('_'):
            otheruri = otheruri[1:]
        return pathto(otheruri, *args, **kw)
    context['pathto'] = gh_pathto

def move_private_folders(app: Sphinx, e: Exception | None) -> None:
    """
    remove leading underscore from folders in in the output folder.

    :todo: should only affect html built
    """
    def join(dir):
        return os.path.join(app.builder.outdir, dir)

    for item in os.listdir(app.builder.outdir):
        if item.startswith('_') and os.path.isdir(join(item)):
            shutil.move(join(item), join(item[1:]))
