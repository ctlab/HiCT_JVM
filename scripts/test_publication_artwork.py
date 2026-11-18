#!/usr/bin/env python3
"""Independent small-field cases for the native RGBA artwork encoding."""
import base64
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET
from PIL import Image
import prepare_publication_artwork as artwork


class NativeCellArtworkTest(unittest.TestCase):
    def assert_partition(self, image):
        rectangles = artwork.pixel_rectangles(image)
        actual = [[None for _ in range(image.width)] for _ in range(image.height)]
        for x, y, w, h, color in rectangles:
            self.assertGreater(w, 0)
            self.assertGreater(h, 0)
            for row in range(y, y + h):
                for col in range(x, x + w):
                    self.assertIsNone(actual[row][col], "Cells overlap")
                    actual[row][col] = color
        for row in range(image.height):
            self.assertEqual(actual[row], [image.getpixel((col, row)) for col in range(image.width)])
        return rectangles

    def test_a_uniform_field_is_one_rectangle(self):
        color = (12, 76, 149, 128)
        self.assertEqual(self.assert_partition(Image.new("RGBA", (3, 4), color)), [[0, 0, 3, 4, color]])

    def test_changing_run_widths_do_not_extend_into_adjacent_cells(self):
        image = Image.new("RGBA", (4, 3))
        image.putdata([(255, 0, 0, 255) if x < limit else (0, 0, 255, 255) for limit in (2, 3, 2) for x in range(4)])
        rectangles = self.assert_partition(image)
        self.assertEqual(len(rectangles), 6)

    def test_checkerboard_and_transparent_rgb_are_retained(self):
        image = Image.new("RGBA", (5, 3))
        image.putdata([((200, 3, 40, 0), (50, 70, 90, 127))[(x + y) % 2] for y in range(3) for x in range(5)])
        self.assert_partition(image)

    def test_single_column_includes_the_last_pixel(self):
        image = Image.new("RGBA", (1, 3))
        image.putdata([(1, 2, 3, 255), (1, 2, 3, 255), (8, 9, 10, 128)])
        self.assertEqual(len(self.assert_partition(image)), 2)

    def scene(self, **attributes):
        image = Image.new("RGBA", (2, 2), (50, 70, 90, 128))
        data = io.BytesIO()
        image.save(data, format="PNG")
        root = ET.Element(f"{{{artwork.NS}}}svg")
        ET.SubElement(root, f"{{{artwork.NS}}}image", {"width": "2", "height": "2", "href": "data:image/png;base64," + base64.b64encode(data.getvalue()).decode(), **attributes})
        return root

    def test_alpha_and_image_translation_survive_the_vector_encoding(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self.scene(transform="matrix(1 0 0 1 -3 7)")
            records = artwork.vectorize_images(root, Path(directory))
            self.assertTrue(records[0]["exactRgbaReconstruction"])
            group = root[0]
            self.assertEqual(group.get("transform"), "matrix(1 0 0 1 -3 7)")
            self.assertEqual(len(group), 1)
            self.assertEqual(group[0].get("fill"), "#32465a")
            self.assertAlmostEqual(float(group[0].get("fill-opacity")), 128 / 255)
            self.assertFalse(list(root.iter(f"{{{artwork.NS}}}image")))

    def test_resized_and_non_embedded_inputs_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "native pixel dimensions"):
                artwork.vectorize_images(self.scene(width="3"), Path(directory))
            with self.assertRaisesRegex(ValueError, "embedded native PNG"):
                artwork.vectorize_images(self.scene(href="https://example.invalid/data.png"), Path(directory))

    def test_the_pixel_budget_is_checked_before_decoding_the_full_field(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(artwork, "MAX_PIXELS", 3):
            with self.assertRaisesRegex(ValueError, "million captured pixels"):
                artwork.vectorize_images(self.scene(), Path(directory))


if __name__ == "__main__":
    unittest.main()
