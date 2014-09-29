require 'pry'
require 'rocksby'

module Rocksby
  class DBWrapper
    def get(key)
      pointer = new_string_pointer
      return nil unless get_helper(key, pointer)

      string = string_from_pointer(pointer)
      delete_string_pointer(pointer)

      return string
    end
  end
end
