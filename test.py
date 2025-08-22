import random
import re

import langid

if __name__ == "__main__":
    str="http://t.meab"
    pattern = r'(https?://[0-9a-zA-Z_+-\.]+)|(@[0-9a-zA-Z_+-]+)'
    str = re.sub(pattern, "", str)

    print(str)
