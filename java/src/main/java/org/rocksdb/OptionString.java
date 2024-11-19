// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@SuppressWarnings("PMD.AvoidStringBufferField")
public class OptionString {
  private static final char kvPairSeparator = ';';
  private static final char kvSeparator = '=';
  private static final char complexValueBegin = '{';
  private static final char complexValueEnd = '}';
  private static final char wrappedValueBegin = '{';
  private static final char wrappedValueEnd = '}';
  private static final char arrayValueSeparator = ':';

  private static final char escapeChar = '\\';

  static class Value {
    final List<String> list;
    final List<Entry> complex;

    public Value(final List<String> list, final List<Entry> complex) {
      this.list = list;
      this.complex = complex;
    }

    public boolean isList() {
      return (this.list != null && this.complex == null);
    }

    public static Value fromList(final List<String> list) {
      return new Value(list, null);
    }

    public static Value fromComplex(final List<Entry> complex) {
      return new Value(null, complex);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder();
      if (isList()) {
        for (final String item : list) {
          sb.append(item).append(arrayValueSeparator);
        }
        // remove the final separator
        if (sb.length() > 0)
          sb.delete(sb.length() - 1, sb.length());
      } else {
        sb.append('[');
        for (final Entry entry : complex) {
          sb.append(entry.toString()).append(';');
        }
        sb.append(']');
      }
      return sb.toString();
    }
  }

  static class Entry {
    public final String key;
    public final Value value;

    private Entry(final String key, final Value value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public String toString() {
      return "" + key + "=" + value;
    }
  }

  static class Parser {
    static class Exception extends RuntimeException {
      private static final long serialVersionUID = 752283782841276408L;

      public Exception(final String s) {
        super(s);
      }
    }

    final String str;
    final StringBuilder sb;

    private Parser(final String str) {
      this.str = str;
      this.sb = new StringBuilder(str);
    }

    private void exception(final String message) {
      final int pos = str.length() - sb.length();
      final int before = Math.min(pos, 64);
      final int after = Math.min(64, str.length() - pos);
      final String here =
          str.substring(pos - before, pos) + "__*HERE*__" + str.substring(pos, pos + after);

      throw new Parser.Exception(message + " at [" + here + "]");
    }

    private void skipWhite() {
      while (sb.length() > 0 && Character.isWhitespace(sb.charAt(0))) {
        sb.delete(0, 1);
      }
    }

    private char first() {
      if (sb.length() == 0)
        exception("Unexpected end of input");
      return sb.charAt(0);
    }

    private char next() {
      if (sb.length() == 0)
        exception("Unexpected end of input");
      final char c = sb.charAt(0);
      sb.delete(0, 1);
      return c;
    }

    private boolean hasNext() {
      return (sb.length() > 0);
    }

    private boolean isChar(final char c) {
      return (sb.length() > 0 && sb.charAt(0) == c);
    }

    private boolean isKeyChar() {
      if (!hasNext())
        return false;
      final char c = first();
      return (Character.isAlphabetic(c) || Character.isDigit(c) || "_".indexOf(c) != -1);
    }

    private boolean isValueChar() {
      if (!hasNext())
        return false;
      final char c = first();
      return (Character.isAlphabetic(c) || Character.isDigit(c) || "_-+.[]".indexOf(c) != -1);
    }

    private String parseKey() {
      final StringBuilder sbKey = new StringBuilder();
      sbKey.append(next());
      while (isKeyChar()) {
        sbKey.append(next());
      }

      return sbKey.toString();
    }

    private String parseSimpleValue() {
      if (isChar(wrappedValueBegin)) {
        next();
        final String result = parseSimpleValue();
        if (!isChar(wrappedValueEnd)) {
          exception("Expected to end a wrapped value with " + wrappedValueEnd);
        }
        next();

        return result;
      } else {
        final StringBuilder sbValue = new StringBuilder();
        while (isValueChar()) sbValue.append(next());

        return sbValue.toString();
      }
    }

    private List<String> parseList() {
      final List<String> list = new ArrayList<>(1);
      while (true) {
        list.add(parseSimpleValue());
        if (isChar(escapeChar)) {
          next();
        }
        if (!isChar(arrayValueSeparator))
          break;

        next();
      }

      return list;
    }

    private Entry parseOption() {
      skipWhite();
      if (!isKeyChar()) {
        exception("No valid key character(s) for key in key=value ");
      }
      final String key = parseKey();
      skipWhite();
      if (isChar(kvSeparator)) {
        next();
      } else {
        exception("Expected = separating key and value");
      }
      skipWhite();
      final Value value = parseValue();
      return new Entry(key, value);
    }

    private Value parseValue() {
      skipWhite();
      if (isChar(complexValueBegin)) {
        next();
        skipWhite();
        final Value value = Value.fromComplex(parseComplex());
        skipWhite();
        if (isChar(complexValueEnd)) {
          next();
          skipWhite();
        } else {
          exception("Expected } ending complex value");
        }
        return value;
      } else if (isValueChar()) {
        return Value.fromList(parseList());
      } else if (isChar(kvPairSeparator)) {
        // e.g. empty vector embedded in a struct option looks like
        // struct_opt = {vector_opt=;...}
        final List<String> entries = new ArrayList<>();
        return Value.fromList(entries);
      }

      exception("No valid value character(s) for value in key=value");
      return null;
    }

    private List<Entry> parseComplex() {
      final List<Entry> entries = new ArrayList<>();

      skipWhite();
      if (hasNext()) {
        entries.add(parseOption());
        skipWhite();
        while (isChar(kvPairSeparator)) {
          next();
          skipWhite();
          if (!isKeyChar()) {
            // the separator was a terminator
            break;
          }
          entries.add(parseOption());
          skipWhite();
        }
      }
      return entries;
    }

    public static List<Entry> parse(final String str) {
      Objects.requireNonNull(str);

      final Parser parser = new Parser(str);
      final List<Entry> result = parser.parseComplex();
      if (parser.hasNext()) {
        parser.exception("Unexpected end of parsing ");
      }

      return result;
    }
  }
}
