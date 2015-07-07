#! /usr/bin/env python
# -*- coding: utf-8 -*-
#
#
# A very simple generator of rasters for testing WKT Raster.
# A test file is a matrix of numbered cells greyscaled randomly.
#
# Requirements: Python + Python Imaging Library (PIL)
# Also, path to FreeSans.ttf is a hardcoded Unix path, so fix it if you need.
#
###############################################################################
# (C) 2009 Mateusz Loskot <mateusz@loskot.net>
# 
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software Foundation,
# Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
###############################################################################
import Image
import ImageDraw
import ImageFont
import random
import sys

if len(sys.argv) < 5 or len(sys.argv) > 6:
    print 'Usage: genraster.py <xsize> <ysize> <xsizecell> <ysizecell> <outline colour>'
    print 'Note: Generated image is saved as out.png'
    sys.exit(1)

g_file = "out.png"
g_size = ( int(sys.argv[1]), int(sys.argv[2]) )
g_cell_size = ( int(sys.argv[3]), int(sys.argv[4]) )
if len(sys.argv) == 6:
    g_outline = int(sys.argv[5])
else:
    g_outline = None

ncells = (g_size[0] / g_cell_size[0]) * (g_size[1] / g_cell_size[1])
print 'Number of cells: ',ncells
print 'ID \tULX\tULY\tCLR\tTXTCLR\tOUTCLR'

img = Image.new("L", g_size, 255)
draw = ImageDraw.Draw(img)

colour_step = 8
count = 0
for j in range(0, g_size[1], g_cell_size[1]):
    for i in range(0, g_size[0], g_cell_size[0]):

        if count < 256 / colour_step:
            value = count * colour_step
        else:
            value = random.randrange(0, 255)

        if value < 16:
            value_text = 255
        else:
            value_text = 0;


        font = ImageFont.truetype('/usr/share/fonts/truetype/freefont/FreeSans.ttf',
                                  g_cell_size[1] - int(g_cell_size[1] * 0.4))
        
        draw.rectangle( [(i,j), (i + g_cell_size[0], j + g_cell_size[1])], fill=value, outline=g_outline)
        draw.text( (i,j), ('%d' % count), fill=value_text, font=font)

        print '%d:\t%d\t%d\t%d\t%d\t%s' % (count, i, j, value, value_text, str(g_outline))
        count = count + 1

del draw
img.save(g_file, 'PNG')
print 'Output saved: %s' % g_file
