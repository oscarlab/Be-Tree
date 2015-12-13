// XGetoptTestDlg.h : header file
//

#ifndef XGETOPTTESTDLG_H
#define XGETOPTTESTDLG_H

#include "XListBox.h"

/////////////////////////////////////////////////////////////////////////////
// CXGetoptTestDlg dialog

class CXGetoptTestDlg : public CDialog
{
// Construction
public:
	CXGetoptTestDlg(CWnd* pParent = NULL);	// standard constructor

// Dialog Data
	//{{AFX_DATA(CXGetoptTestDlg)
	enum { IDD = IDD_XGETOPTTEST_DIALOG };
	CXListBox	m_List;
	CEdit		m_CommandLine;
	//}}AFX_DATA

	// ClassWizard generated virtual function overrides
	//{{AFX_VIRTUAL(CXGetoptTestDlg)
protected:
	virtual void DoDataExchange(CDataExchange* pDX);	// DDX/DDV support
	//}}AFX_VIRTUAL

// Implementation
protected:
	HICON m_hIcon;
	BOOL ProcessCommandLine(int argc, TCHAR *argv[]);

	// Generated message map functions
	//{{AFX_MSG(CXGetoptTestDlg)
	virtual BOOL OnInitDialog();
	afx_msg void OnSysCommand(UINT nID, LPARAM lParam);
	afx_msg void OnPaint();
	afx_msg HCURSOR OnQueryDragIcon();
	afx_msg void OnGetopt();
	//}}AFX_MSG
	DECLARE_MESSAGE_MAP()
};

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.

#endif //XGETOPTTESTDLG_H
