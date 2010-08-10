module Paperclip
	module Interpolations
		extend self

		def param attachment, style_name
			attachment.instance.to_param
		end
	end
end