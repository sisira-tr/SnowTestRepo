from matplotlib.backends.backend_pdf import PdfPages


class PDFWriter:

    def write(self, figs, config):
        # path = f"/tmp/{config.report_name}.pdf"
        path = f"{config.report_name}.pdf"

        with PdfPages(path) as pdf:
            for fig in figs:
                pdf.savefig(fig)

        return path
