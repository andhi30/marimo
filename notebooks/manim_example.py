import marimo

__generated_with = "0.12.9"
app = marimo.App(width="medium")


@app.cell
def _():
    from manim import BLUE, GREEN, PINK, Circle, Create, Scene, Square, Transform
    from manim_slides import Slide
    from manim_slides.ipython import ipython_magic as ipython_magic_slides

    class CircleToSquare(Slide):
        def construct(self):
            blue_circle = Circle(color=BLUE, fill_opacity=0.5)
            green_square = Square(color=GREEN, fill_opacity=0.8)
            self.play(Create(blue_circle))
            self.next_slide()

            self.play(Transform(blue_circle, green_square))

    ipython_magic_slides.ManimSlidesMagic({}).manim_slides(
        """CircleToSquare""",
        None,
        {
            "CircleToSquare": CircleToSquare,
            "config": {"media_embed": True},
        },
    )
    return (
        BLUE,
        Circle,
        CircleToSquare,
        Create,
        GREEN,
        PINK,
        Scene,
        Slide,
        Square,
        Transform,
        ipython_magic_slides,
    )


if __name__ == "__main__":
    app.run()
