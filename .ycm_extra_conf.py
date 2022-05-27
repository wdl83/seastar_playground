import platform
import os.path as p
import subprocess

DIR_OF_THIS_SCRIPT = p.abspath( p.dirname( __file__ ) )
SOURCE_EXTENSIONS = [ '.cpp', '.cxx', '.cc', '.c' ]

flags = [
'-DTRACE_ENABLED',
'-Wall',
'-Werror',
'-Wextra',
'-Wno-ignored-qualifiers',
'-Wno-parentheses',
'-Wno-unused-parameter',
'-Wunreachable-code'
'-fcoroutines',
'-std=c++20'
]

def pkg_config(pkg):
  def not_whitespace(string):
    return not (string == '' or string == '\n')
  output = subprocess.check_output(['pkg-config', '--cflags', pkg], universal_newlines=True).strip()
  return filter(not_whitespace, output.split(' '))

flags += pkg_config('seastar/build/debug/seastar.pc')

def IsHeaderFile( filename ):
  extension = p.splitext( filename )[ 1 ]
  return extension in [ '.h', '.hxx', '.hpp', '.hh' ]


def FindCorrespondingSourceFile( filename ):
  if IsHeaderFile( filename ):
    basename = p.splitext( filename )[ 0 ]
    for extension in SOURCE_EXTENSIONS:
      replacement_file = basename + extension
      if p.exists( replacement_file ):
        return replacement_file
  return filename

def Settings( **kwargs ):
  # Do NOT import ycm_core at module scope.
  import ycm_core

  language = kwargs[ 'language' ]

  if language == 'cfamily':
    filename = FindCorrespondingSourceFile( kwargs[ 'filename' ] )

    return {
      'flags': flags,
      'include_paths_relative_to_dir': DIR_OF_THIS_SCRIPT,
      'override_filename': filename
    }

  return {}
