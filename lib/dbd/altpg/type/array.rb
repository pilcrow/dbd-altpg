
require 'dbd/altpg/type/util'

module DBI::DBD::AltPg::Type
  class Array
    # int32 ndim .............. # of dimensions
    # int32 flags ............. ignored for now
    # oid type, ............... basic element type
    # [int32 dim, int32 lbound] x ndim ......
    # elem ...

    # derived classes set @scalar_type ...

    def self.parse(bytes)
      return nil if bytes.nil?

      ndim, type = [ bytes[0,4], bytes[8,4] ].map { |i|
        Util.unpack_int32_big(i)
      }

      #element_type = @type_map[ type.unpack('N')[0][:binary] ]
      dim_extents = (0 .. ndim-1).collect { |i|
        Util.unpack_int32_big(bytes[12 + i*8, 4])
        # Ignore "lbound" -- ruby arrays start at zero
      }
      nelem = dim_extents.inject(1) { |a,b| a *= b }

      offset = 12 + ndim * 8
      elem = 0.upto(nelem - 1).collect{ |i|
        sz = Util.unpack_int32_big(bytes[offset,4])
        obj = @element_type.parse(bytes[offset += 4, sz])
        offset += sz
        obj
      }

      dim_extents.delete_at(0)
      dim_extents.reverse_each do |len|
        tmp = []
        while elem.length > 0
          tmp << elem.slice!(0, len)
        end
        elem = tmp
      end
      elem
    end
  end # -- class Array
end
